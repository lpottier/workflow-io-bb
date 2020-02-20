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

XBT_LOG_NEW_DEFAULT_CATEGORY(bb_scheduler, "Log category for BB Scheduler");

/**
 * @brief Create a JobScheduler with two types of storages PFS and BB
 */
BBJobScheduler::BBJobScheduler(const FileMap_t &file_placement, const std::string& nb_cores) : 
                file_placement(file_placement), nb_cores(nb_cores) {}

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

  if (compute_services.size() != 1) {
    throw std::runtime_error("This example BB Scheduler requires a single compute service");
  }

  auto compute_service = *compute_services.begin();

  WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());
  
  std::map< std::string, std::string> tasks_resource_mapping;
  std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> > file_locations;

  for (auto task : tasks) {
    for (auto tuple : this->file_placement) {
      file_locations[std::get<0>(tuple)] = std::get<2>(tuple);
    }
    tasks_resource_mapping[task->getID()] = this->nb_cores;
  }
  
  wrench::WorkflowJob *job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(tasks, file_locations);
  this->getJobManager()->submitJob(job, compute_service, tasks_resource_mapping);

  WRENCH_INFO("Done with scheduling tasks as standard jobs");
}
// Old version with bb_stage tasks
// void BBJobScheduler::scheduleTasks(
//             const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
//             const std::vector<wrench::WorkflowTask *> &tasks) {

//   // Check that the right compute_services is passed
//   //TODO: MAKE SURE MULTIPLE CS CAN BE USED
//   if (compute_services.size() != 1) {
//     throw std::runtime_error("This example BB Scheduler requires a single compute service");
//   }

//   auto compute_service = *compute_services.begin();

//   std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> > file_locations_after_stagein;
//   std::map< std::string, std::string> tasks_resource_mapping;

//   for (auto tuple : this->file_placement)
//     file_locations_after_stagein[std::get<0>(tuple)] = std::get<2>(tuple);


//   WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());
//   for (auto task : tasks) {
//     wrench::WorkflowJob *job = nullptr;
//     std::vector<wrench::WorkflowTask *> vec_task;
//     vec_task.push_back(task);

//     // for (auto tuple : file_locations_after_stagein)
//     //   std::cout << "DEBUG | " << task->getID() << " | " << tuple.first->getID() << " | " << tuple.second->getHostname() << std::endl;

//     // We prepare the stage-in of input files
//     if (task->getID() == "bb_stagein") {
//       // We build the input files required by the job from the original 
//       // file_placement by selecting only the input files

//       // We run stage-in/out task with only one core
//       tasks_resource_mapping[task->getID()] = "1";

//       std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> > input_files;
//       FileMap_t input_files_to_copy;
//       for (auto tuple : this->file_placement) {
//         // If it is an input file for one task
//         if (!std::get<0>(tuple)->isOutput()) {
//           input_files[std::get<0>(tuple)] = std::get<1>(tuple);
//           // We only insert a file to stage if the src storage and the dest storage are different
//           // If we ask to copy a file to/from the same storage, simgrid fails
//           if (std::get<1>(tuple)->getHostname() != std::get<2>(tuple)->getHostname())
//             input_files_to_copy.insert(tuple);
//         }
//       }
//       job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(vec_task, input_files, {}, input_files_to_copy, {});
//     }
//     else if (task->getID() == "bb_stageout") {
//       // We run stage-in/out task with only one core
//       tasks_resource_mapping[task->getID()] = "1";
//       // Build a reverse file allocation to stage out the files (i.e., the src StorageService becomes the dest and vice-versa)
//       FileMap_t reverse_file_placement;
//       for (auto elem : this->file_placement) {
//         if (std::get<1>(elem)->getHostname() != std::get<2>(elem)->getHostname())
//           reverse_file_placement.insert(std::make_tuple(std::get<0>(elem), std::get<2>(elem), std::get<1>(elem)));
//       }

//       job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(vec_task, file_locations_after_stagein, {}, reverse_file_placement, {});
//       reverse_file_placement.clear();
//     }
//     else {
//       tasks_resource_mapping[task->getID()] = this->nb_cores;
//       job = (wrench::WorkflowJob *) this->getJobManager()->createStandardJob(vec_task, file_locations_after_stagein, {}, {}, {});
//     }
//     this->getJobManager()->submitJob(job, compute_service, tasks_resource_mapping);
//     vec_task.clear();
//   }
//   WRENCH_INFO("Done with scheduling tasks as standard jobs");
// }


