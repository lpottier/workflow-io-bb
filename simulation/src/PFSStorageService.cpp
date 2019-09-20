/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include "PFSStorageService.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(pfs_storage, "Log category for PFS Storage");

/**
 * @brief Public constructor
 *
 * @param hostname: the name of the host on which to start the service
 * @param capacity: the storage capacity in bytes
 * @param linkspeed: the storage bandwidth in bytes per second
 * @param linklatency: the storage latency in micro second
 * @param files: files that should be allocated in that storage
 * @param property_list: a property list ({} means "use all defaults")
 * @param messagepayload_list: a message payload list ({} means "use all defaults")
 */
PFSStorageService::PFSStorageService(const std::string& hostname,
        double capacity,
        double linkspeed,
        double linklatency,
        const std::set<wrench::WorkflowFile*>& files,
        std::map<std::string, std::string> property_list,
        std::map<std::string, double> messagepayload_list) :
            wrench::SimpleStorageService(hostname, 
                capacity, 
                property_list, 
                messagepayload_list), 
            linkspeed(linkspeed),
            linklatency(linklatency),
            files(files) {}
