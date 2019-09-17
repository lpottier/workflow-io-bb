/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef MY_UTILS_PRINT_H
#define MY_UTILS_PRINT_H

#include <limits>
#include <simgrid/s4u.hpp>

#include <wrench-dev.h>

typedef std::numeric_limits< double > dbl;

void printWorkflowTTY(const std::string& workflow_id, 
                      wrench::Workflow* workflow);

void printWorkflowFile(const std::string& workflow_id, 
                       wrench::Workflow* workflow, 
                       const std::string& output,
                       const char sep = ' ');

void printFileAllocationTTY(const std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> >& file_placements);
void printWMSFileAllocationTTY(const std::shared_ptr<wrench::WMS>& wms);

void printSimulationSummaryTTY(wrench::SimulationOutput& simulation_output);

void printHostRouteTTY(const std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> >& hostpair_to_link);

void printHostStorageAssociationTTY(const std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*>& map);

#endif