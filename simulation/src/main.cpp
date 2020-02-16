/**
 * Copyright (c) 2019. Loïc Pottier.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include <wrench.h>

#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <cstddef>           // for std::size_t

#include <simgrid/s4u.hpp>

#include "BBSimulation.h"
#include "BBDecision.h"

int main(int argc, char **argv) {

  // Parsing of the command-line arguments for this WRENCH simulation
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file> <stage list file> <output dir>" << std::endl;
    exit(1);
  }

  // The first argument is the platform description file, written in XML following the SimGrid-defined DTD
  std::string platform_file(argv[1]);
  // The second argument is the workflow description file, written in XML using the DAX DTD
  std::string workflow_file(argv[2]);
  // The third argument is the list of files to stage (whiccd h files go in BB)
  // The format is "file_src file_dest", one file per line.
  // if this file is empty or does not exist -> all files in PFS
  std::string stage_list(argv[3]);
  // The third argument is the output directory (where to write the simulation results)
  std::string output_dir(argv[4]);

  // Declaration of the top-level WRENCH simulation object
  BBSimulation simulation(platform_file, workflow_file, stage_list, output_dir);

  // Initialization of the simulation
  simulation.init(&argc, argv);

  //MAKE SURE THE DIR EXIST/HAS PERM

  // Reading and parsing the workflow description file to create a wrench::Workflow object
  // and the platform description file to instantiate a simulated platform
  wrench::Workflow *workflow = simulation.parse_inputs();

  std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*>> route;
  route = simulation.create_hosts();

  // printHostStorageAssociationTTY(cs_to_pfs);
  // printHostStorageAssociationTTY(cs_to_bb);

  //printHostRouteTTY(route);

  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services = simulation.instantiate_storage_services();

  // Create a list of compute services that will be used by the WMS
  // Instantiate a bare metal service and add it to the simulation
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services = simulation.instantiate_compute_services();


  //All services run on the main PFS node (by rule PFSHost1)
  wrench::FileRegistryService* file_registry_service = simulation.instantiate_file_registry_service();


  //////////////////////// Stage the chosen files from PFS to BB -> here heuristics or given
  FileMap_t file_placement_heuristic;
  std::shared_ptr<wrench::StorageService> first_bb_node = *(simulation.getBBServices()).begin();

  // Parse the file containing the list of files to stage in
  auto files_to_stages = BBSimulation::parseFilesList(stage_list, simulation.getPFSService(), first_bb_node);

  int nb_files_staged = 0;
  float amount_of_data_staged = 0.0;
  /* All files in PFS */
  for (auto f : workflow->getFiles()) {
    // If not found files stay in PFS by default
    if (files_to_stages.count(f->getID()) == 0) {
      std::cerr << "[INFO] file " << f->getID() << " not found in " << stage_list << ". This file stays in the PFS." << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), simulation.getPFSService()));
    }
    else {
      if (files_to_stages[f->getID()]->getHostname() != "PFSHost1"){
        nb_files_staged += 1;
        amount_of_data_staged += f->getSize();
      }
      std::cerr << "[INFO] file " << f->getID() << " will be staged in " << files_to_stages[f->getID()]->getHostname() << std::endl;
      file_placement_heuristic.insert(std::make_tuple(f, simulation.getPFSService(), files_to_stages[f->getID()]));
    }
  }

  std::cout << nb_files_staged << "/" 
            << workflow->getFiles().size() 
            << " files staged in BB (" 
            << amount_of_data_staged/std::pow(2,20) 
            << " MB)." << std::endl;

  /* One file in first BB node */ 
  // for (auto f : workflow->getFiles()) {
  //   file_placement_heuristic.insert(std::make_tuple(
  //                                   f, 
  //                                   simulation.getPFSService(), 
  //                                   first_bb_node
  //                                   )
  //                             );
  //   break;
  // }

  /* All BB heuristic : as we can have multiple BB : *(simulation.getBBServices()).begin() */ 
  // for (auto f : workflow->getFiles()) {
  //   file_placement_heuristic.insert(std::make_tuple(
  //                                   f, 
  //                                   simulation.getPFSService(), 
  //                                   *(simulation.getBBServices()).begin()
  //                                   )
  //                             );
  // }

  //printFileAllocationTTY(file_placement_heuristic);
  ////////////////////////

  // EFT schedule_heuristic;
  // InitialFileAlloc initFileAlloc(file_placement_heuristic);

  // It is necessary to store, or "stage", input files in the PFS
  std::pair<int, double> stagein_fstat = simulation.stage_input_files();

  // auto ftest = *(workflow->getFiles()).begin();
  // initFileAlloc(ftest);

  std::shared_ptr<wrench::WMS> wms = simulation.instantiate_wms_service(file_placement_heuristic);

  // !!! TODO !!! in the doc about Summit ->  (Note that a compute service can be associated to a "by default" storage service upon instantiation);

  //printWorkflowTTY(workflow_id, workflow);
  //printWorkflowFile(workflow_id, workflow, output_dir + "/workflow-stat.csv");
  simulation.dumpWorkflowStatCSV();

  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  auto& simulation_output = simulation.getOutput();

  printSimulationSummaryTTY(simulation_output);

  //simulation.dumpAllOutputJSON();

  //SEGFAULT ?
  //std::cout << simulation.getHostName() << std::endl;

  return 0;
}

