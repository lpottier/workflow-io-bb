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
#include <iomanip>

#include "BBWMS.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(simple_wms, "Log category for BB WMS");

/**
 * @brief Create a WMS with a workflow instance, a scheduler implementation, and a list of compute services
 */
BBWMS::BBWMS(std::unique_ptr<wrench::StandardJobScheduler> standard_job_scheduler,
                     std::unique_ptr<wrench::PilotJobScheduler> pilot_job_scheduler,
                     const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                     const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                     const std::set<std::shared_ptr<wrench::StorageService>> &pfs_storage_services,
                     const std::set<std::shared_ptr<wrench::StorageService>> &bb_storage_services,
                     std::shared_ptr<wrench::FileRegistryService> file_registry_service,
                     const std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> &file_placements,
                     const std::string &hostname) : wrench::WMS(
         std::move(standard_job_scheduler),
         std::move(pilot_job_scheduler),
         compute_services,
         storage_services,
         {}, file_registry_service,
         hostname,
         "bbwms"),
         pfs_storage_services(pfs_storage_services),
         bb_storage_services(bb_storage_services),
         file_placements(file_placements) {}

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

  //Transform file_placements into a dict for 
  std::map<std::string, std::shared_ptr<wrench::StorageService>> file_placements_str;
  for (auto alloc : file_placements)
     file_placements_str[alloc.first->getID()] = alloc.second;

  if (!file_registry)
    throw std::runtime_error("No File Registry Service available");

  std::cout << std::right << std::setw(45) << "===    STAGE IN    ===" << std::endl;
  auto pfs_storage = *(this->pfs_storage_services.begin());

  // //Move specified files from PFS to BB
  // for (auto elem : file_placements) {
  //   auto attached_storages = file_registry->lookupEntry(elem.first);

  //   // for (auto storage : attached_storages)
  //   //   std::cout << elem.first->getID() << " -- " << elem.second->getHostname() << " -> " << storage->getHostname() << std::endl;

  //   if (attached_storages.size() > 1) {
  //     WRENCH_INFO("The file (%s) belongs to more than one storage (max. authorized -> one storage)",
  //                  (elem.first->getID().c_str()));
  //     throw std::runtime_error("Aborting");
  //   }

  //   if (attached_storages.size() == 1) { 
  //     auto current_storage = *attached_storages.begin();
  //     //TODO add exception management 
  //     std::cerr << "File " << elem.first->getID() << " is staged in " << current_storage->getHostname() << std::endl;
  //     if (current_storage->getHostname() != elem.second->getHostname()) {
  //       data_movement_manager->doSynchronousFileCopy(elem.first, current_storage, elem.second, file_registry);
  //       current_storage->deleteFile(elem.first, file_registry);
  //     }
  //   }
  // }

  for (auto file : this->getWorkflow()->getFiles()) {
      if(pfs_storage->lookupFile(file)) {
        data_movement_manager->doSynchronousFileCopy(file, pfs_storage, 
                                        file_placements_str[file->getID()], 
                                        file_registry);
        pfs_storage->deleteFile(file, file_registry);      
      }
  }

  //print current files allocation
  std::cout << std::left << std::setw(30) << "FILE" << std::setw(20) << 
               std::left << "STORAGE" << std::endl;
  std::cout << std::left << std::setw(30) << "----" << std::setw(20) << 
               std::left << "-------" << std::endl;
  for (auto storage : this->getAvailableStorageServices()) {
    for (auto file : this->getWorkflow()->getFiles()) {
      if(storage->lookupFile(file)) {
        std::cout << std::left << std::setw(30) << 
                    file->getID() << std::setw(20) << 
                    std::left << storage->getHostname() << std::endl;
      }
    }
  }

  std::cout << std::right << std::setw(45) << "===    SIMULATION    ===" << std::endl;

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

  //Move specified files from PFS to BB
  for (auto bb : this->bb_storage_services) {
    for (auto file : this->getWorkflow()->getFiles()) {
      if(bb->lookupFile(file)) {
        data_movement_manager->doSynchronousFileCopy(file, bb, pfs_storage, file_registry);
        bb->deleteFile(file, file_registry); // MAYBE OPTIONAL
      }
    }
  }

  std::cout << std::right << std::setw(45) << "===    STAGE OUT    ===" << std::endl;
  //print file_registry
  std::cout << std::left << std::setw(30) << "FILE" << std::setw(20) << 
               std::left << "STORAGE" << std::endl;
  std::cout << std::left << std::setw(30) << "----" << std::setw(20) << 
               std::left << "-------" << std::endl;

  for (auto storage : this->getAvailableStorageServices()) {
    for (auto file : this->getWorkflow()->getFiles()) {
      if(storage->lookupFile(file)) {
        std::cout << std::left << std::setw(30) << file->getID() 
                  << std::setw(20) << std::left << storage->getHostname() 
                  << std::endl;
      }
    }
  }

  //wrench::S4U_Simulation::sleep(10);

  this->job_manager.reset();

  return 0;
}
