/**
 * Copyright (c) 2019. Loïc Pottier.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include "BBJobScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(simple_scheduler, "Log category for BB Scheduler");

/**
 * @brief Create a JobScheduler with two types of storages PFS and BB
 */
BBJobScheduler::BBJobScheduler(
  const std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> &file_placements) :
         file_placements(file_placements) {}

/**
 * @brief Schedule and run a set of ready tasks on available cloud resources
 *
 * @param compute_services: a set of compute services available to run jobs
 * @param tasks: a map of (ready) workflow tasks
 *
 * @throw std::runtime_error
 */
void BBJobScheduler::scheduleTasks(
            const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
            const std::vector<wrench::WorkflowTask *> &tasks) {

  // Check that the right compute_services is passed
  //TODO: MAKE SURE MULTIPLE CS CAN BE USED
  if (compute_services.size() != 1) {
    throw std::runtime_error("This example BB Scheduler requires a single compute service");
  }

  auto compute_service = *compute_services.begin();

  WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());
  for (auto task : tasks) {
    wrench::WorkflowJob *job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(task, this->file_placements);
    this->getJobManager()->submitJob(job, compute_service);
  }
  WRENCH_INFO("Done with scheduling tasks as standard jobs");
}

