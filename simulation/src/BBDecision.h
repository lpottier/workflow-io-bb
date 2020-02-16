/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBDECISION_H
#define MY_BBDECISION_H


#include <iostream>
#include <functional>

#include <wrench.h>

#include "BBTypes.h"

class SchedulingDecision {
public:
    virtual std::set<std::pair<wrench::WorkflowTask *, std::shared_ptr<wrench::ComputeService>>> 
    operator()(const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services, const std::vector<wrench::WorkflowTask *> &tasks) const = 0;
};

class FileDecision {
public:
    FileDecision(const FileMap_t& _init_file_alloc) : init_file_alloc(_init_file_alloc) {};
    virtual std::shared_ptr<wrench::StorageService>
    operator()(wrench::WorkflowFile* file) const = 0;
    const FileMap_t& getInitAlloc() const { return this->init_file_alloc;}
private:
    // std::shared_ptr<wrench::WMS> wms;
    FileMap_t init_file_alloc;
};

class EFT : public SchedulingDecision {
public:
    std::set<std::pair<wrench::WorkflowTask *, std::shared_ptr<wrench::ComputeService>>>
    operator()(const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services, const std::vector<wrench::WorkflowTask *> &tasks) const
    {
        for (auto t: tasks) {
            std::cerr << "EFT: "<< t->getID() << std::endl;
        }

        return {};
    }
};

class InitialFileAlloc : public FileDecision {
public:
    InitialFileAlloc(const FileMap_t& _init_file_alloc) : FileDecision(_init_file_alloc) {};
    
    std::shared_ptr<wrench::StorageService>
    operator()(wrench::WorkflowFile* file) const
    {
        std::cerr << "initFileAlloc: " << file->getID() << std::endl;

        for (auto elem : this->getInitAlloc()) {
            if (file->getID() == std::get<0>(elem)->getID())
                return std::get<2>(elem);
        }

        return nullptr;
    }
};

//std::function<std::shared_ptr<wrench::StorageService>(wrench::WorkflowFile*)> data_alloc = DataAllocation();

// const wrench::Workflow& -> std::shared_ptr<FileMap_t>
//std::function<std::shared_ptr<FileMap_t>(const wrench::Workflow&)> choice;

#endif