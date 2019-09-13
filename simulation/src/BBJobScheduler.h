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

#include <wrench-dev.h>

class BBJobScheduler : public wrench::StandardJobScheduler {
public:
  BBJobScheduler(const std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> &file_placements);

  void scheduleTasks(
       const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
       const std::vector<wrench::WorkflowTask *> &tasks);

private:
  std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> file_placements;
};

#endif //MY_BBSCHEDULER_H

