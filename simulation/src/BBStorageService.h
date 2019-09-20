/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBSTORAGESERVICE_H
#define MY_BBSTORAGESERVICE_H

#include "BBTypes.h"

#include <wrench-dev.h>

class BBStorageService : public wrench::SimpleStorageService {
public:
  BBStorageService(const std::string& hostname,
            double capacity,
            double linkspeed,
            double linklatency,
            const std::shared_ptr<wrench::StorageService>& pfs_storage,
            const std::set<wrench::WorkflowFile*>& files,
            std::map<std::string, std::string> property_list = {},
            std::map<std::string, double> messagepayload_list = {});

  const double getLinkSpeed() const { return this->linkspeed; }
  const double getLinkLatency() const { return this->linklatency; }

  std::shared_ptr<wrench::StorageService> getPFSStorage() const { 
    return this->pfs_storage;
  }

  const std::set<wrench::WorkflowFile*> getFiles() const { 
    return this->files;
  }

  const bool type() const { 
    return STORAGE_TYPE::BB;
  }

private:
    double linkspeed; //In GB/s
    double linklatency; //in microsecond
    std::shared_ptr<wrench::StorageService> pfs_storage;
    //std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService>> data_placement;
    std::set<wrench::WorkflowFile*> files;
};

#endif //MY_BBSTORAGESERVICE_H
