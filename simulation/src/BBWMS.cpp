/**
 * Copyright (c) 2019. Loïc Pottier.
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
                     const std::string &hostname) : wrench::WMS(
         std::move(standard_job_scheduler),
         std::move(pilot_job_scheduler),
         compute_services,
         storage_services,
         {}, nullptr,
         hostname,
         "bbwms"),
         pfs_storage_services(pfs_storage_services),
         bb_storage_services(bb_storage_services) {}


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

  std::cout << std::right << std::setw(45) << "===    STAGE IN    ===" << std::endl;
  //print current files allocation
  this->printFileAllocationTTY();

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

  //wrench::S4U_Simulation::sleep(10);

  this->job_manager.reset();


  std::cout << std::right << std::setw(45) << "===    STAGE OUT    ===" << std::endl;
  this->printFileAllocationTTY();

  return 0;
}

void BBWMS::printFileAllocationTTY() {
  auto precision = std::cout.precision();
  std::cout.precision(std::numeric_limits< double >::max_digits10);

  //print current files allocation
  std::cout << std::left << std::setw(31) 
            << " FILE"
            << std::left << std::setw(20)
            << " STORAGE"
            << std::left << std::setw(30) 
            << " SIZE(GB)" << std::endl;
  std::cout << std::left << std::setw(31) 
            << " ----"
            << std::left << std::setw(20)
            << " -------"
            << std::left << std::setw(30) 
            << " --------" << std::endl;
  for (auto storage : this->getAvailableStorageServices()) {
    for (auto file : this->getWorkflow()->getFiles()) {
      if(storage->lookupFile(file)) {
        std::cout << " " << std::left << std::setw(31) 
                  << file->getID() << std::setw(20) 
                  << std::left << storage->getHostname() 
                  << std::left << std::setw(30)
                  << file->getSize()/std::pow(2,30) << std::endl;
      }
    }
  }
  std::cout.flush();
  //back to previous precision
  std::cout.precision(precision);
}
