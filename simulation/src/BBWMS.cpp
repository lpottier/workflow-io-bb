/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include <iostream>

#include "BBWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(simple_wms, "Log category for BB WMS");

/**
 * @brief Create a WMS with a workflow instance, a scheduler implementation, and a list of compute services
 */
BBWMS::BBWMS(std::unique_ptr<wrench::StandardJobScheduler> standard_job_scheduler,
                     std::unique_ptr<wrench::PilotJobScheduler> pilot_job_scheduler,
                     const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                     const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                     std::shared_ptr<wrench::FileRegistryService> file_registry_service,
                     const std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> &file_locations,
                     const std::string &hostname) : wrench::WMS(
         std::move(standard_job_scheduler),
         std::move(pilot_job_scheduler),
         compute_services,
         storage_services,
         {}, file_registry_service,
         hostname,
         "bbwms"),
         file_locations(file_locations) {}

/**
 * @brief main method of the BBWMS daemon
 */
int BBWMS::main() {

  wrench::TerminalOutput::setThisProcessLoggingColor(wrench::TerminalOutput::COLOR_GREEN);

  // Check whether the WMS has a deferred start time
  checkDeferredStart();

  WRENCH_INFO("About to execute a workflow with %lu tasks", this->getWorkflow()->getNumberOfTasks());

  // Create a job manager
  this->job_manager = this->createJobManager();

  // Create a data movement manager
  std::shared_ptr<wrench::DataMovementManager> data_movement_manager = this->createDataMovementManager();

  std::shared_ptr<wrench::FileRegistryService> file_registry = this->getAvailableFileRegistryService();

  if (!file_registry)
    throw std::runtime_error("No FileRegistryService running");

  //Move data from PFS to BB according the given partition
  for (auto elem : file_locations) {
    auto attached_storages = file_registry->lookupEntry(elem.first);

    for (auto storage : attached_storages)
      std::cout << elem.first->getID() << " -- " << elem.second->getHostname() << " -> " << storage->getHostname() << std::endl;

    std::cout << attached_storages.size() << std::endl;

    if (attached_storages.size() != 1) {
      WRENCH_INFO("The file (%s) belongs to more than one storage (max. authorized -> one storage)",
                   (elem.first->getID().c_str()));
      //throw std::runtime_error("Aborting");
    }

    // auto pfs_storage = attached_storages.begin(); // first and only element

    //std::cout << pfs_storage << std::endl;

    //data_movement_manager->doSynchronousFileCopy(elem.first, pfs_storage, elem.second);
  }

  while (true) {
    // Get the ready tasks
    std::vector<wrench::WorkflowTask *> ready_tasks = this->getWorkflow()->getReadyTasks();

    // Get the available compute services
    auto compute_services = this->getAvailableComputeServices<wrench::ComputeService>();

    if (compute_services.empty()) {
      WRENCH_INFO("Aborting - No compute services available!");
      break;
    }

    // Run ready tasks with defined scheduler implementation
    this->getStandardJobScheduler()->scheduleTasks(this->getAvailableComputeServices<wrench::ComputeService>(), ready_tasks);

    // Wait for a workflow execution event, and process it
    try {
      this->waitForAndProcessNextEvent();
    } catch (wrench::WorkflowExecutionException &e) {
      WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                   (e.getCause()->toString().c_str()));
      continue;
    }

    if (this->getWorkflow()->isDone()) {
      break;
    }
  }

  //Move back the data from the BB to the PFS according the given partition
  // for (auto elem : file_locations) {
  //   attached_storages = lookupEntry(elem.first);
  //   if (attached_storages.size() != 1) {
  //     WRENCH_INFO("The file (%s) belongs to %s storages (file must belongs to PFS)",
  //                  (elem.first->getID().c_str()));
  //     throw std::runtime_error("Aborting");
  //   }

  //   auto bb_storage = *attached_storages.begin(); // first and only element

  //   data_movement_manager->doSynchronousFileCopy(elem.first, bb_storage, elem.second);
  // }
  wrench::S4U_Simulation::sleep(10);

  this->job_manager.reset();

  return 0;
}
