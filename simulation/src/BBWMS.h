/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBWMS_H
#define MY_BBWMS_H

#include <wrench-dev.h>

class Simulation;

/**
 *  @brief A Burst Buffer WMS implementation
 */
class BBWMS : public wrench::WMS {
public:
    BBWMS(std::unique_ptr<wrench::StandardJobScheduler> standard_job_scheduler,
              std::unique_ptr<wrench::PilotJobScheduler> pilot_job_scheduler,
              const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
              const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
              const std::set<std::shared_ptr<wrench::StorageService>> &pfs_storage_services,
              const std::set<std::shared_ptr<wrench::StorageService>> &bb_storage_services,
              std::shared_ptr<wrench::FileRegistryService> file_registry_service,
              const std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> &file_placements,
              const std::string &hostname);

private:
    int main() override;

    /** @brief The job manager */
    std::shared_ptr<wrench::JobManager> job_manager;
    std::set<std::shared_ptr<wrench::StorageService>> pfs_storage_services;
    std::set<std::shared_ptr<wrench::StorageService>> bb_storage_services;
    std::map<wrench::WorkflowFile *, std::shared_ptr<wrench::StorageService>> file_placements;
};

#endif //MY_BBWMS_H

