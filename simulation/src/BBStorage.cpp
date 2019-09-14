/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include "BBStorage.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(bb_storage, "Log category for BB Storage");

/**
 * @brief Public constructor
 *
 * @param hostname: the name of the host on which to start the service
 * @param capacity: the storage capacity in bytes
 * @param linkspeed: the storage bandwidth in bytes per second
 * @param root_storage: the storage on top of the BB in the hierarchy (usually the PFS)
 * @param property_list: a property list ({} means "use all defaults")
 * @param messagepayload_list: a message payload list ({} means "use all defaults")
 */
BBStorage::BBStorage(const std::string& hostname,
        double capacity,
        double linkspeed,
        const std::shared_ptr<wrench::SimpleStorageService>& root_storage,
        std::map<std::string, std::string> property_list,
        std::map<std::string, double> messagepayload_list) :
            wrench::SimpleStorageService(hostname, 
                capacity, 
                property_list, 
                messagepayload_list), 
            linkspeed(linkspeed),
            root_storage(root_storage) {}

double BBStorage::stageInFiles() {

    return 0.0;
}

double BBStorage::stageOutFiles() {

    return 0.0;
}
