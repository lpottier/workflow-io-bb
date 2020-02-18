/**
 * Copyright (c) 2019. Loïc Pottier.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBSCHEDULER_H
#define MY_BBSCHEDULER_H

#include "BBTypes.h"

#include <wrench-dev.h>

class BBJobScheduler : public wrench::StandardJobScheduler {
public:
  BBJobScheduler(const FileMap_t &file_placement, const std::string& nb_cores);

  void scheduleTasks(
       const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
       const std::vector<wrench::WorkflowTask *> &tasks);

private:
  FileMap_t file_placement;
  std::string nb_cores; // Core per task (same for all of them)
  // std::shared_ptr<wrench::StorageService> default_storage_service;
};

#endif //MY_BBSCHEDULER_H

