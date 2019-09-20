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

#include <simgrid/s4u.hpp>

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

static bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}


int main(int argc, char **argv) {
  // Declaration of the top-level WRENCH simulation object
  wrench::Simulation simulation;

  // Initialization of the simulation
  simulation.init(&argc, argv);

  // Parsing of the command-line arguments for this WRENCH simulation
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file> <output dir>" << std::endl;
    exit(1);
  }

  // The first argument is the platform description file, written in XML following the SimGrid-defined DTD
  std::string platform_file(argv[1]);
  // The second argument is the workflow description file, written in XML using the DAX DTD
  std::string workflow_file(argv[2]);
  // The third argument is the output directory (where to write the simulation results)
  std::string output_dir(argv[3]);

  std::size_t workflowf_pos = workflow_file.find_last_of("/");
  std::size_t platformf_pos = platform_file.find_last_of("/");

  std::string workflow_id = workflow_file.substr(workflowf_pos+1);
  std::string platform_id = platform_file.substr(platformf_pos+1);


  //MAKE SURE THE DIR EXIST/HAS PERM

  /* Reading and parsing the workflow description file to create a wrench::Workflow object */
  wrench::Workflow *workflow;
  if (ends_with(workflow_file, "dax")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1000Gf");
  } else if (ends_with(workflow_file,"json")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1000Gf");
  } else {
      std::cerr << "Workflow file name must end with '.dax' or '.json'" << std::endl;
      exit(1);
  }
  std::cout << "The workflow has " << workflow->getNumberOfTasks() << " tasks " << std::endl;
  auto sizefiles = workflow->getFiles();

  double totsize = 0;
  for (auto f : sizefiles)
    totsize += f->getSize();
  std::cout << "Total files size " << totsize << " Bytes (" << totsize/std::pow(2,40) << " TB)" << std::endl;  
  std::cout.flush();



  // Reading and parsing the platform description file to instantiate a simulated platform
  simulation.instantiatePlatform(platform_file);

  // Get a vector of all the hosts in the simulated platform
  std::vector<std::string> hostname_list = simulation.getHostnameList();
  // std::set<std::string> hostname_set(hostname_list.begin(), hostname_list.end());

  // Construct a list of execution hosts (i.e., compute node) and storage hosts (name, size, bandwidth, latency)
  std::set<std::string> execution_hosts;
  std::set<std::tuple<std::string, double, double, double>> pfs_hosts;
  std::set<std::tuple<std::string, double, double, double>> bb_hosts;

  //Structures that maintain hosts-links information (host_src, host_dest) -> Link
  std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> > hostpair_to_link;
  // Pair of compute hosts, BB hosts. By definition all compute hosts have access to the PFS
  std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_attached_to_bb;
  std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> cs_attached_to_pfs;

  double total_bb_size = 0;
  //Read all hosts and create a list of compute nodes and storage nodes
  for (auto host : hostname_list) {
    simgrid::s4u::Host* simhost = simgrid::s4u::Host::by_name(host);
    std::string host_type = std::string(simhost->get_property("type"));

    for (auto dest : hostname_list) {
      if (host == dest) continue;
      std::vector<simgrid::s4u::Link*> route;
      simgrid::s4u::Host* host_dest = simgrid::s4u::Host::by_name(dest);
      std::string host_dest_type = std::string(host_dest->get_property("type"));

      simhost->simgrid::s4u::Host::route_to(host_dest, route, nullptr);

      hostpair_to_link[std::make_pair(host,dest)] = route;

      // If the route is going from a compute node to a storage node
      // route.size() == 1 is here to ensure that we consider a route with only one hop
      // It is a requirement in the private BB case (Summit)
      if (host_type == std::string(COMPUTE_NODE) && 
          host_dest_type == std::string(STORAGE_NODE) &&
          route.size() == 1) {
        std::string host_dest_category = std::string(host_dest->get_property("category"));
        if (host_dest_category == std::string(PFS_NODE)) {
          cs_attached_to_pfs[std::make_pair(host,dest)] = route[0];
        }
        else if (host_dest_category == std::string(BB_NODE)) {
          cs_attached_to_bb[std::make_pair(host,dest)] = route[0];
        }
      }

      route.clear();
    }

    if (host_type == std::string(COMPUTE_NODE)) {
      execution_hosts.insert(host);
    }
    else if (host_type == std::string(STORAGE_NODE)) {
      std::string size = std::string(simhost->get_property("size"));
      std::string category = std::string(simhost->get_property("category"));

      if (category == std::string(PFS_NODE)) {
        pfs_hosts.insert(std::make_tuple(host, std::stod(size)));

        // auto host_service = simulation.add(new PFSStorageService(
        //                                             host, 
        //                                             std::stod(size),
        //                                             0.0,
        //                                             {})
        //                                   );
        // storage_services.insert(host_service);
        // pfs_storage_services.insert(host_service);
      }
      else if (category == std::string(BB_NODE)) {
        bb_hosts.insert(std::make_tuple(host, std::stod(size)));
        total_bb_size += std::stod(size);
        // auto host_service = simulation.add(new BBStorageService(
        //                                             host,
        //                                             std::stod(size),
        //                                             0.0,
        //                                             nullptr,
        //                                             {})
        //                                   );

        // storage_services.insert(host_service);
        // bb_storage_services.insert(host_service);
      }
    }
  }

  // for (auto pair : cs_attached_to_pfs) {
  //   std::cout << "CS: " << pair.first.first << " attached to PFS:" << pair.first.second 
  //             << " bandwidth:" << pair.second->get_bandwidth()
  //             << " latency:" << pair.second->get_latency() 
  //             << std::endl;
  // }

  // for (auto pair : cs_attached_to_bb) {
  //   std::cout << "CS: " << pair.first.first << " attached to BB:" << pair.first.second 
  //             << " bandwidth:" << pair.second->get_bandwidth()
  //             << " latency:" << pair.second->get_latency() 
  //             << std::endl;
  // }

  printHostStorageAssociationTTY(cs_attached_to_pfs);
  printHostStorageAssociationTTY(cs_attached_to_bb);

  //printHostRouteTTY(hostpair_to_link);

  if (pfs_hosts.size() != 1)
    throw std::runtime_error("This simulation requires exactly one PFS host");

  if (execution_hosts.empty()) {
    throw std::runtime_error("This simulation requires at least one compute node in the platform file");
  }

  if (bb_hosts.empty()) {
    throw std::runtime_error("This simulation requires at least two storage nodes in the platform file");
  }

  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services;
  std::shared_ptr<PFSStorageService> pfs_storage_service;
  std::set<std::shared_ptr<BBStorageService>> bb_storage_services;

  std::string pfs_host = std::get<0>(*pfs_hosts.begin()); // Only one PFS is allowed for now
  
  try {
    pfs_storage_service = simulation.add(new PFSStorageService(
                                              pfs_host, 
                                              std::get<1>(*pfs_hosts.begin()),
                                              0.0,
                                              {})
                                      );
    storage_services.insert(pfs_storage_service);
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  try {
    for (auto bbhost : bb_hosts) {
      auto service = simulation.add(new BBStorageService(
                                            std::get<0>(bbhost),
                                            std::get<1>(bbhost),
                                            0.0,
                                            pfs_storage_service,
                                            {})
                                  );
      storage_services.insert(service);
      bb_storage_services.insert(service);
    }
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }  

  // Create a list of compute services that will be used by the WMS
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
  // Instantiate a bare metal service and add it to the simulation
  try {
    auto baremetal_service = new wrench::BareMetalComputeService(
          pfs_host, execution_hosts, 0, {}, {});

    compute_services.insert(simulation.add(baremetal_service));
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  //All services run on the main PFS node (by rule PFSHost1)
  // Instantiate a file registry service
  try {
    auto file_registry_service = new wrench::FileRegistryService(pfs_host);
    simulation.add(file_registry_service);

  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  FileMap_t file_placement_heuristic;

  //////////////////////// Stage the chosen files from PFS to BB -> here heuristics
  //std::map<wrench::WorkflowFile*, std::shared_ptr<wrench::StorageService> > file_placements;

  for (auto f : workflow->getFiles()) {
    // file_placements[f] = *pfs_storage_services.begin();
    //file_placements[f] = *bb_storage_services.begin();
    file_placement_heuristic.insert(std::make_tuple(
                                    f, 
                                    pfs_storage_service, 
                                    *bb_storage_services.begin()
                                    )
                              );
  }

  printFileAllocationTTY(file_placement_heuristic);
  ////////////////////////

  // It is necessary to store, or "stage", input files in the PFS
  auto input_files = workflow->getInputFiles();
  try {
    simulation.stageFiles(input_files, pfs_storage_service);
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  // Instantiate a WMS
  auto wms = simulation.add(
          new BBWMS(
            std::unique_ptr<BBJobScheduler>(new BBJobScheduler(file_placement_heuristic)),
            nullptr, compute_services, 
            storage_services,
            pfs_host)
          );
  wms->addWorkflow(workflow);

  // !!! TODO !!! in the doc about Summit ->  (Note that a compute service can be associated to a "by default" storage service upon instantiation);

  printWorkflowTTY(workflow_id, workflow);
  printWorkflowFile(workflow_id, workflow, output_dir + "/workflow-stat.csv");

  std::cout.precision(std::numeric_limits< double >::max_digits10);
  std::cout << "before the simulation: " << simulation.getCurrentSimulatedDate() << std::endl;

  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  std::cout << "before the simulation: " << simulation.getCurrentSimulatedDate() << std::endl;

  auto& simulation_output = simulation.getOutput();
  printSimulationSummaryTTY(simulation_output);

  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks;
  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampFileCopyCompletion> *> trace_copyfiles;

  trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();
  trace_copyfiles = simulation_output.getTrace<wrench::SimulationTimestampFileCopyCompletion>();

  simulation_output.dumpPlatformGraphJSON(output_dir + "/platform.json");
  simulation_output.dumpWorkflowExecutionJSON(workflow, output_dir + "/execution.json", false);
  // simulation_output->dumpWorkflowExecutionJSON(workflow, output_dir + "/execution-layout.json", true);
  simulation_output.dumpWorkflowGraphJSON(workflow, output_dir + "/workflow.json");

  auto stage_in = trace_tasks[0]->getContent()->getTask();
  auto stage_out = trace_tasks[trace_tasks.size()-1]->getContent()->getTask();

  std::cout << "Task in first trace entry: " << stage_in->getID() << " " << stage_in->getStartDate() << " " << stage_in->getEndDate() << " " << stage_in->getEndDate()-stage_in->getStartDate() << std::endl;
  std::cout << "Task in last trace entry: " << stage_out->getID() << " " << stage_out->getStartDate() << " " << stage_out->getEndDate() << " " << stage_out->getEndDate() - stage_out->getStartDate() << std::endl;

  //SEGFAULT ?
  //std::cout << simulation.getHostName() << std::endl;

  return 0;
}

