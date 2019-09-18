/**
 * Copyright (c) 2019. Loïc Pottier.
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

#include "Types.h"

class Simulation;

// struct FileAllocation {
//     wrench::WorkflowFile* file,
//     std::shared_ptr<wrench::StorageService> src,
//     std::shared_ptr<wrench::StorageService> dst,
// };

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
              const std::string &hostname);

    void printFileAllocationTTY();

    void stageInFilesBB(bool deleteFileAfter = true);

    void stageOutFilesPFS(bool deleteFileAfter = true);

private:
    int main() override;

    /** @brief The job manager */
    std::shared_ptr<wrench::JobManager> job_manager;
    std::set<std::shared_ptr<wrench::StorageService>> pfs_storage_services;
    std::set<std::shared_ptr<wrench::StorageService>> bb_storage_services;
};

#endif //MY_BBWMS_H

