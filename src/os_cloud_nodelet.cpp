/**
 * Copyright (c) 2018-2023, Ouster, Inc.
 * All rights reserved.
 *
 * @file os_cloud_nodelet.cpp
 * @brief A nodelet to publish point clouds and imu topics
 */

// prevent clang-format from altering the location of "ouster_ros/os_ros.h", the
// header file needs to be the first include due to PCL_NO_PRECOMPILE flag
// clang-format off
#include "ouster_ros/os_ros.h"
// clang-format on

#include <nodelet/nodelet.h>
#include <pluginlib/class_list_macros.h>
#include <ros/console.h>
#include <ros/ros.h>
#include <std_msgs/String.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/PointCloud2.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <cassert>

#include "ouster_ros/PacketMsg.h"
#include "os_transforms_broadcaster.h"
#include "imu_packet_handler.h"
#include "lidar_packet_handler.h"
#include "point_cloud_processor.h"
#include "laser_scan_processor.h"

namespace sensor = ouster::sensor;

namespace ouster_ros {

class OusterCloud : public nodelet::Nodelet {
   public:
    OusterCloud() : tf_bcast(getName()) {}

   private:
    virtual void onInit() override {
        create_metadata_subscriber();
        NODELET_INFO("OusterCloud: nodelet created!");
    }

    void create_metadata_subscriber() {
        metadata_sub = getNodeHandle().subscribe<std_msgs::String>(
            "metadata", 1, &OusterCloud::metadata_handler, this);
    }

    void metadata_handler(const std_msgs::String::ConstPtr& metadata_msg) {
        // TODO: handle sensor reconfigurtion
        NODELET_INFO("OusterCloud: retrieved new sensor metadata!");

        auto info = sensor::parse_metadata(metadata_msg->data);

        tf_bcast.parse_parameters(getPrivateNodeHandle());

        auto dynamic_transforms =
            getPrivateNodeHandle().param("dynamic_transforms_broadcast", false);
        auto dynamic_transforms_rate = getPrivateNodeHandle().param(
            "dynamic_transforms_broadcast_rate", 1.0);
        if (dynamic_transforms && dynamic_transforms_rate < 1.0) {
            NODELET_WARN(
                "OusterCloud: dynamic transforms enabled but wrong rate is "
                "set, clamping to 1 Hz!");
            dynamic_transforms_rate = 1.0;
        }

        if (!dynamic_transforms) {
            NODELET_INFO("OusterCloud: using static transforms broadcast");
            tf_bcast.broadcast_transforms(info);
        } else {
            NODELET_INFO_STREAM(
                "OusterCloud: dynamic transforms broadcast enabled wit "
                "broadcast rate of: "
                << dynamic_transforms_rate << " Hz");
            timer_ = getNodeHandle().createTimer(
                ros::Duration(1.0 / dynamic_transforms_rate),
                [this, info](const ros::TimerEvent&) {
                    tf_bcast.broadcast_transforms(info, last_msg_ts);
                });
        }

        create_publishers_subscribers(info);
    }

    void create_publishers_subscribers(const sensor::sensor_info& info) {
        auto& pnh = getPrivateNodeHandle();
        auto proc_mask = pnh.param("proc_mask", std::string{"IMU|PCL|SCAN"});
        auto tokens = parse_tokens(proc_mask, '|');

        auto timestamp_mode_arg = pnh.param("timestamp_mode", std::string{});
        bool use_ros_time = timestamp_mode_arg == "TIME_FROM_ROS_TIME";

        auto& nh = getNodeHandle();
        sub_time_stamp = nh.subscribe("/sensor_sync_node/trigger_2", 10, &OusterCloud::trigger_stamp_cb, this, ros::TransportHints().tcpNoDelay());

        if (check_token(tokens, "IMU")) {
            imu_pub = nh.advertise<sensor_msgs::Imu>("imu", 100);
            imu_packet_handler = ImuPacketHandler::create_handler(
                info, tf_bcast.imu_frame_id(), use_ros_time);
            imu_packet_sub = nh.subscribe<PacketMsg>(
                "imu_packets", 100, [this](const PacketMsg::ConstPtr msg) {
                    auto imu_msg = imu_packet_handler(msg->buf.data());
                    if (imu_msg.header.stamp > last_msg_ts)
                        last_msg_ts = imu_msg.header.stamp;
                    
                    // std::cout << "Handling imu: " << msg.header.stamp.sec << std::endl;
                    ros::Time corrected_stamp;
                    static uint32_t err_count = 0;
                    if (imu_msg.header.stamp.sec >= 5)
                    {
                      std::lock_guard<std::mutex> lock(time_stamp_queue_mutex);
                      // Since the buffer overflows at 4.something, handling the count before 5 would require additional logic. 
                      // Instead, we just skip the first 4 pointclouds and then start with the simpler logic.
                      while (!time_stamp_queue.empty())
                      {
                        uint32_t trigger_count = std::stoul(time_stamp_queue.front().frame_id);
                        // std::cout << "\tchecking: " << trigger_count << std::endl;

                        if (trigger_count == imu_msg.header.stamp.sec)
                        {
                          // std::cout << "\tmatch found" << std::endl;
                          corrected_stamp.fromSec(time_stamp_queue.front().stamp.toSec() + imu_msg.header.stamp.nsec / 1e9);
                          break;
                        }
                        else if (trigger_count < imu_msg.header.stamp.sec)
                        {
                          // std::cout << "\tlidar is larger, popping " << trigger_count << std::endl;
                          // This is the only safe time to pop since the timestamp reported from the lidar is monotonically increasing
                          time_stamp_queue.pop();
                        }
                        else
                        {
                          ++err_count;
                        }
                      }
                    }
                    // std::cout << "\toriginal ts: " << imu_msg.header.stamp.toSec() << std::endl;
                    // std::cout << "\tcorrected ts: " << corrected_stamp.toSec() << std::endl;
                    // std::cout << "\tqueue size: " << time_stamp_queue.size() << std::endl;
                    if (err_count)
                    {
                      std::cout << "\terror_count: " << err_count << std::endl;
                    }

                    imu_msg.header.stamp = corrected_stamp;

                    imu_pub.publish(imu_msg);
                });
        }

        int num_returns = get_n_returns(info);

        std::vector<LidarScanProcessor> processors;
        if (check_token(tokens, "PCL")) {
            lidar_pubs.resize(num_returns);
            for (int i = 0; i < num_returns; ++i) {
                lidar_pubs[i] = nh.advertise<sensor_msgs::PointCloud2>(
                    topic_for_return("points", i), 10);
            }

            processors.push_back(PointCloudProcessor::create(
                info, tf_bcast.point_cloud_frame_id(),
                tf_bcast.apply_lidar_to_sensor_transform(),
                [this](PointCloudProcessor::OutputType msgs) {
                    for (size_t i = 0; i < msgs.size(); ++i) {
                        if (msgs[i]->header.stamp > last_msg_ts)
                            last_msg_ts = msgs[i]->header.stamp;
                        lidar_pubs[i].publish(*msgs[i]);
                    }
                }));
        }

        if (check_token(tokens, "SCAN")) {
            scan_pubs.resize(num_returns);
            for (int i = 0; i < num_returns; ++i) {
                scan_pubs[i] = nh.advertise<sensor_msgs::LaserScan>(
                    topic_for_return("scan", i), 10);
            }

            // TODO: avoid duplication in os_cloud_node
            int beams_count = static_cast<int>(get_beams_count(info));
            int scan_ring = pnh.param("scan_ring", 0);
            scan_ring = std::min(std::max(scan_ring, 0), beams_count - 1);
            if (scan_ring != pnh.param("scan_ring", 0)) {
                NODELET_WARN_STREAM(
                    "scan ring is set to a value that exceeds available range"
                    "please choose a value between [0, " << beams_count <<
                    "], ring value clamped to: " << scan_ring);
            }

            processors.push_back(LaserScanProcessor::create(
                info,
                tf_bcast
                    .point_cloud_frame_id(),  // TODO: should we allow having a
                                              // different frame for the laser
                                              // scan than point cloud???
                scan_ring, [this](LaserScanProcessor::OutputType msgs) {
                    for (size_t i = 0; i < msgs.size(); ++i) {
                        if (msgs[i]->header.stamp > last_msg_ts)
                            last_msg_ts = msgs[i]->header.stamp;
                        scan_pubs[i].publish(*msgs[i]);
                    }
                }));
        }

        if (check_token(tokens, "PCL") || check_token(tokens, "SCAN")) {
            lidar_packet_handler = LidarPacketHandler::create_handler(
                info, use_ros_time, processors);
            lidar_packet_sub = nh.subscribe<PacketMsg>(
                "lidar_packets", 100, [this](const PacketMsg::ConstPtr msg) {
                    lidar_packet_handler(msg->buf.data());
                });
        }
    }

    void trigger_stamp_cb(const std_msgs::HeaderConstPtr& msg)
    {
      std::lock_guard<std::mutex> lock(time_stamp_queue_mutex);
      time_stamp_queue.push(*msg);
    }

   private:
    ros::Subscriber metadata_sub;
    ros::Subscriber imu_packet_sub;
    ros::Publisher imu_pub;
    ros::Subscriber lidar_packet_sub;
    std::vector<ros::Publisher> lidar_pubs;
    std::vector<ros::Publisher> scan_pubs;

    OusterTransformsBroadcaster tf_bcast;
    ros::Subscriber sub_time_stamp;
    std::queue<std_msgs::Header> time_stamp_queue;
    std::mutex time_stamp_queue_mutex;
    const std::string node_ready_param_name{"/ready"};

    ImuPacketHandler::HandlerType imu_packet_handler;
    LidarPacketHandler::HandlerType lidar_packet_handler;

    ros::Timer timer_;
    ros::Time last_msg_ts;
};

}  // namespace ouster_ros

PLUGINLIB_EXPORT_CLASS(ouster_ros::OusterCloud, nodelet::Nodelet)
