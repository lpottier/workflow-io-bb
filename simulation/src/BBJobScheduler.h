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
  BBJobScheduler(std::shared_ptr<wrench::StorageService> default_storage_service) :
          default_storage_service(default_storage_service) {}

  void scheduleTasks(const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                     const std::vector<wrench::WorkflowTask *> &tasks);

private:
  std::shared_ptr<wrench::StorageService> default_storage_service;
};

#endif //MY_BBSCHEDULER_H

