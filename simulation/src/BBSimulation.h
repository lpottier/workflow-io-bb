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

/**
 *  @brief A Burst Buffer Simulation
 */
class BBSimulation : public wrench::Simulation {
public:
    BBSimulation(
                const std::string& jobid,
                const std::string& id, 
                const std::string& pipeline,
                const std::string& max_pipeline,
                const std::string& cores,
                const std::string& fits,
                const std::string& bb_type,
                const std::string& platform_file,
                const std::string& workflow_file,
                const std::string& stage_list,
                const std::string& real_log,
                const std::string& makespan,
                const std::string& output_dir
    );

    void init(int *argc, char **argv);
    wrench::Workflow* parse_inputs();

    std::map<std::string, std::shared_ptr<wrench::StorageService> > 
        parseFilesList(std::string, 
            std::shared_ptr<wrench::StorageService>, 
            std::shared_ptr<wrench::StorageService>
    );
    std::map<std::string, double> parseRealWorkflowLog(std::string path);

    /* Getter */
    const std::string getJobID() const { return this->simulation_job_id;}
    const std::string getID() const { return this->simulation_id;}
    const std::string getNumberPipeline() const { return this->nb_pipeline;}
    const std::string getMaxPipeline() const { return this->max_pipeline;}
    const std::string getNumberCores() const { return this->nb_cores;}
    const double getMeasuredMakespan() const { return this->measured_makespan;}

    const std::string getWorkflowID() const { return this->workflow_id;}
    const std::string getPlatformID() const { return this->platform_id;}
    const double getRealWallTime() { return this->real_data_run["TOTAL"];}

    const double getBandwithPFS() { return std::get<0>(this->pfs_link);}
    const double getLatencyPFS() { return std::get<1>(this->pfs_link);}

    const double getBandwithBB() { return std::get<0>(this->bb_link);}
    const double getLatencyBB() { return std::get<1>(this->bb_link);}

    const std::string getFITS() { return this->fits;}
    const std::string getBBtype() { return this->bb_type;}

    const int getNBFileStaged() const { return this->nb_files_staged;}
    const double getDataStaged() const { return this->amount_of_data_staged;}
    void setNBFileStaged(int x) { this->nb_files_staged = x;}
    void setDataStaged(double x) { this->amount_of_data_staged = x;}

    const int getNBFileInBB() const { return this->nb_files_in_bb;}
    const double getDataInBB() const { return this->amount_of_data_in_bb;}
    void setNBFileInBB(int x) { this->nb_files_in_bb = x;}
    void setDataInBB(double x) { this->amount_of_data_in_bb = x;}


    std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*>> create_hosts();
    std::set<std::shared_ptr<wrench::StorageService>> instantiate_storage_services();
    std::set<std::shared_ptr<wrench::ComputeService>> instantiate_compute_services();
    wrench::FileRegistryService* instantiate_file_registry_service();
    std::pair<int, double> stage_input_files();
    std::shared_ptr<wrench::WMS> instantiate_wms_service(const FileMap_t& file_placement_heuristic);

    std::shared_ptr<PFSStorageService> getPFSService() { return this->pfs_storage_service; }
    std::set<std::shared_ptr<BBStorageService>> getBBServices() { return this->bb_storage_services; }
    std::set<std::shared_ptr<wrench::StorageService>> getStorageServices() { return this->storage_services; }
    std::set<std::shared_ptr<wrench::ComputeService>> getComputeServices() { return this->compute_services; }

    void dumpAllOutputJSON(bool layout = false);
    void dumpWorkflowStatCSV();
    void dumpResultPerTaskCSV(const std::string& output, char sep = ' ');

private:
    std::pair<double, double> check_links(std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> route);

    std::map<std::string, std::string> raw_args;
    std::string simulation_id;
    std::string simulation_job_id;
    std::string nb_pipeline;
    std::string max_pipeline;
    std::string nb_cores;
    std::string fits;
    std::string bb_type;

    double measured_makespan;

    std::string workflow_id;
    std::string platform_id;

    //Contains the measured walltime for each task on a machine corresponding to the platform
    std::map<std::string, double> real_data_run;
    std::pair<double, double> pfs_link; //std::pair<bandwith, latency>
    std::pair<double, double> bb_link; //std::pair<bandwith, latency>

    int nb_files_staged; // number of files staged in BB
    double amount_of_data_staged; // Amount of data staged in BB (Bytes)


    int nb_files_in_bb; // number of files in BB
    double amount_of_data_in_bb; // Amount of data in BB (Bytes)

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

    // Structures that maintain hosts-links information (host_src, host_dest) -> Link
    std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> > hostpair_to_link;
    std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_to_pfs;
    std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_to_bb;

};

#endif //MY_BBSIMULATION_H
