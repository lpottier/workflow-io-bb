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
                     const std::string &hostname) : wrench::WMS(
         std::move(standard_job_scheduler),
         std::move(pilot_job_scheduler),
         compute_services,
         storage_services,
         {}, nullptr,
         hostname,
         "bbwms") {}


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

  std::cerr << std::right << std::setw(45) << "===    ADD STAGE-IN/OUT TASKS    ===" << std::endl;
  //print current files allocation
  //this->printFileAllocationTTY();

  // this->addStageInTask("bb_stagein");
  // this->addStageOutTask("bb_stageout");

  std::cerr << std::right << std::setw(45) << "===    SIMULATION    ===" << std::endl;

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

  std::cerr << std::right << std::setw(45) << "===    END SIMULATION    ===" << std::endl;
  //this->printFileAllocationTTY();

  return 0;
}

void BBWMS::addStageInTask(const std::string& task_id, unsigned int parallelization) {
  wrench::WorkflowTask* bb_stagein = this->getWorkflow()->addTask(task_id, 1.0, 1, wrench::ComputeService::ALL_CORES, 1.0,  1);

  // Retrieve the first level of tasks to connect the new BB tasks
  //int nb_level = this->getWorkflow()->getNumLevels();
  std::vector<wrench::WorkflowTask*> first_tasks = this->getWorkflow()->getTasksInTopLevelRange(0,0);
  // Connect the BB stage in task to the first tasks in the workflow
  for (auto task : first_tasks) {
    this->getWorkflow()->addControlDependency(bb_stagein, task);
  }
}

void BBWMS::addStageOutTask(const std::string& task_id, unsigned int parallelization) {
  wrench::WorkflowTask* bb_stageout = this->getWorkflow()->addTask(task_id, 1.0, 1, wrench::ComputeService::ALL_CORES, 1.0, 1);

  // Retrieve the last tasks to connect the new BB tasks
  int nb_level = this->getWorkflow()->getNumLevels();
  std::vector<wrench::WorkflowTask*> last_tasks = this->getWorkflow()->getTasksInTopLevelRange(nb_level-1,nb_level-1);  

  // Connect the BB stage out task to the last tasks in the workflow
  for (auto task : last_tasks) {
    this->getWorkflow()->addControlDependency(task, bb_stageout);
  }
}

void BBWMS::printFileAllocationTTY() {
  auto precision = std::cout.precision();
  std::cout.precision(std::numeric_limits< double >::max_digits10);

  int width_name = 50;
  int width_storage = 20;
  int width_size = 30;

  //print current files allocation
  std::cout << std::left << std::setw(width_name+1) 
            << " FILE"
            << std::left << std::setw(width_storage)
            << " STORAGE"
            << std::left << std::setw(width_size) 
            << " SIZE(MB)" << std::endl;
  std::cout << std::left << std::setw(width_name+1) 
            << " ----"
            << std::left << std::setw(width_storage)
            << " -------"
            << std::left << std::setw(width_size) 
            << " --------" << std::endl;
  for (auto storage : this->getAvailableStorageServices()) {
    for (auto file : this->getWorkflow()->getFiles()) {
      if(storage->lookupFile(file)) {
        std::cout << " " << std::left << std::setw(width_name+1) 
                  << file->getID() << std::setw(width_storage) 
                  << std::left << storage->getHostname() 
                  << std::left << std::setw(width_size)
                  << file->getSize()/std::pow(2,20) << std::endl;
      }
    }
  }
  std::cout.flush();
  //back to previous precision
  std::cout.precision(precision);
}
