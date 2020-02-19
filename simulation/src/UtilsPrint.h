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

#include "BBTypes.h"

class BBSimulation;

#include <simgrid/s4u.hpp>
#include <wrench-dev.h>

bool ends_with(const std::string& str, const std::string& suffix);

void printWorkflowTTY(const std::string& workflow_id, 
                      wrench::Workflow* workflow);

void printWorkflowFile(const std::string& workflow_id, 
                       wrench::Workflow* workflow, 
                       const std::string& output,
                       const char sep = ' ');

void printFileAllocationTTY(const FileMap_t& file_placement);
void printWMSFileAllocationTTY(const std::shared_ptr<wrench::WMS>& wms);

void printSimulationSummaryTTY(BBSimulation& simulation, bool header = true);
void printSimulationSummaryCSV(BBSimulation& simulation, std::string file_name, bool header = true);

void printHostRouteTTY(const std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> >& hostpair_to_link);

void printHostStorageAssociationTTY(const std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*>& map);

#endif