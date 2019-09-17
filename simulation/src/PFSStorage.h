/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_PFSSTORAGE_H
#define MY_PFSSTORAGE_H

#include <wrench-dev.h>

class PFSStorage : public wrench::SimpleStorageService {
public:
  PFSStorage(const std::string& hostname,
            double capacity,
            double linkspeed,
            const std::set<wrench::WorkflowFile*>& files,
            std::map<std::string, std::string> property_list = {},
            std::map<std::string, double> messagepayload_list = {});

  const double getLinkSpeed() const { return this->linkspeed; }

  const std::set<wrench::WorkflowFile*> getFiles() const { 
    return this->files;
  }

  const bool isBB() const { 
    return false;
  }

private:
    double linkspeed;
    //std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService>> data_placement;
    std::set<wrench::WorkflowFile*> files;
};

#endif //MY_PFSSTORAGE_H

