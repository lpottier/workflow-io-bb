/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBSIMULATION_H
#define MY_BBSIMULATION_H


#include <iostream>
#include <iomanip>
#include <fstream>

#include <simgrid/s4u.hpp>

#include <wrench-dev.h>

#include "BBTypes.h"
#include "BBStorageService.h"
#include "PFSStorageService.h"
#include "BBJobScheduler.h"
#include "BBWMS.h"
#include "UtilsPrint.h"

#define COMPUTE_NODE "compute"
#define STORAGE_NODE "storage"
#define PFS_NODE "PFS"
#define BB_NODE "BB"
#define PFS_LINK "pfslink"
#define BB_LINK "bblink"

class Simulation;

/**
 *  @brief A Burst Buffer Simulation
 */
class BBSimulation : public wrench::Simulation {
public:
    BBSimulation(int argc, char **argv,
                 const std::string& platform_file,
                 const std::string& workflow_file,
                 const std::string& output_dir);

    std::set<std::shared_ptr<wrench::StorageService>> instantiate_storage_services();
    std::set<std::shared_ptr<wrench::ComputeService>> instantiate_compute_services();
    wrench::FileRegistryService* instantiate_file_registry_service();
    void stage_input_files();
    std::shared_ptr<wrench::WMS> instantiate_wms_service(const FileMap_t& file_placement_heuristic);

private:
    bool ends_with(const std::string& str, const std::string& suffix);
    void parse_input(wrench::Workflow *workflow);
    void create_hosts();
    std::pair<double, double> check_links(std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> route);

    std::map<std::string, std::string> raw_args;
    std::string workflow_id;
    std::string platform_id;

    wrench::Workflow *workflow;
    std::shared_ptr<wrench::WMS> wms;

    std::string wms_host;
    std::string file_registry_service_host;
    wrench::FileRegistryService* file_registry_service;
    std::tuple<std::string, double> pfs_storage_host;
    std::shared_ptr<PFSStorageService> pfs_storage_service;
    std::set<std::tuple<std::string, double>> bb_storage_hosts;

    std::set<std::shared_ptr<BBStorageService>> bb_storage_services;
    std::set<std::shared_ptr<wrench::StorageService>> storage_services;

    std::set<std::string> execution_hosts;
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> > hostpair_to_link;
    std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_to_pfs;
    std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_to_bb;
};

#endif //MY_BBSIMULATION_H