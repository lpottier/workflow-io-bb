/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBSCHEDULER_H
#define MY_BBSCHEDULER_H

#include <wrench-dev.h>

class BBJobScheduler : public wrench::StandardJobScheduler {
public:
  BBJobScheduler(const std::set<std::shared_ptr<wrench::StorageService> > &pfs_storage_services,
                 const std::set<std::shared_ptr<wrench::StorageService> > &bb_storage_services);

  void scheduleTasks(const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                     const std::vector<wrench::WorkflowTask *> &tasks);

private:
  std::set<std::shared_ptr<wrench::StorageService> > pfs_storage_services;
  std::set<std::shared_ptr<wrench::StorageService> > bb_storage_services;
};

#endif //MY_BBSCHEDULER_H

