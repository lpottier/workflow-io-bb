/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
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
 * @brief Schedule and run a set of ready tasks on available cloud resources
 *
 * @param compute_services: a set of compute services available to run jobs
 * @param tasks: a map of (ready) workflow tasks
 *
 * @throw std::runtime_error
 */
void BBJobScheduler::scheduleTasks(const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                                               const std::vector<wrench::WorkflowTask *> &tasks) {

  // Check that the right compute_services is passed
  if (compute_services.size() != 1) {
    throw std::runtime_error("This example BB Scheduler requires a single compute service");
  }

  auto compute_service = *compute_services.begin();
  // std::shared_ptr<wrench::BatchComputeService> batch_service;
  // if (not(batch_service = std::dynamic_pointer_cast<wrench::BatchComputeService>(compute_service))) {
  //   throw std::runtime_error("This example Batch Scheduler can only handle a batch service");
  // }

  WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());
  for (auto task : tasks) {
    std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> file_locations;
    for (auto f : task->getInputFiles()) {
      file_locations.insert(std::make_pair(f, default_storage_service));
    }
    for (auto f : task->getOutputFiles()) {
      file_locations.insert(std::make_pair(f, default_storage_service));
    }

    wrench::WorkflowJob *job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(task, file_locations);
    // std::map<std::string, std::string> batch_job_args;
    // batch_job_args["-N"] = "1";
    // batch_job_args["-t"] = "2000000"; //time in minutes
    // batch_job_args["-c"] = "1"; //number of cores per node
    // this->getJobManager()->submitJob(job, batch_service, batch_job_args);
    this->getJobManager()->submitJob(job, compute_service);
  }
  WRENCH_INFO("Done with scheduling tasks as standard jobs");
}

