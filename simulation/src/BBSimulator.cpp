/**
 * Copyright (c) 2019. <ADD YOUR HEADER INFORMATION>.
 * Generated with the wrench-init tool.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include <wrench.h>

#include <simgrid/s4u.hpp>

#include "BBJobScheduler.h"
#include "BBWMS.h"

#define PFS_HOST "PFSHost0"
#define BB_HOST "BBHost0"
#define COMPUTE_NODE "compute"

static bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}

int main(int argc, char **argv) {

  // Declaration of the top-level WRENCH simulation object
  wrench::Simulation simulation;

  // Initialization of the simulation
  simulation.init(&argc, argv);

  // Parsing of the command-line arguments for this WRENCH simulation
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file>" << std::endl;
    exit(1);
  }

  // The first argument is the platform description file, written in XML following the SimGrid-defined DTD
  char *platform_file = argv[1];
  // The second argument is the workflow description file, written in XML using the DAX DTD
  char *workflow_file = argv[2];


  /* Reading and parsing the workflow description file to create a wrench::Workflow object */
  std::cerr << "Loading workflow..." << std::endl;
  wrench::Workflow *workflow;
  if (ends_with(workflow_file, "dax")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1000Gf");
  } else if (ends_with(workflow_file,"json")) {
      workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1000Gf");
  } else {
      std::cerr << "Workflow file name must end with '.dax' or '.json'" << std::endl;
      exit(1);
  }
  std::cerr << "The workflow has " << workflow->getNumberOfTasks() << " tasks " << std::endl;
  std::cerr.flush();

  // Reading and parsing the platform description file to instantiate a simulated platform
  simulation.instantiatePlatform(platform_file);

  if (!wrench::S4U_Simulation::hostExists(BB_HOST)) {
    throw std::runtime_error("This simulation requires a BBHost0 in the platform file");
  }
  if (!wrench::S4U_Simulation::hostExists(PFS_HOST)) {
    throw std::runtime_error("This simulation requires a PFSHost0 in the platform file");
  }
  // Get a vector of all the hosts in the simulated platform
  std::vector<std::string> hostname_list = simulation.getHostnameList();
  // std::set<std::string> hostname_set(hostname_list.begin(), hostname_list.end());

  simgrid::s4u::Host* bbhost = simgrid::s4u::Host::by_name(BB_HOST);
  std::string bbtype = std::string(bbhost->get_property("type"));
  std::string bbsize = std::string(bbhost->get_property("size"));

  std::cerr << "BB storage " << bbtype << " of size " << bbsize << " GB " << std::endl;
  std::cerr.flush();

  // Instantiate a storage service
  std::string pfs_storage_host = PFS_HOST;
  auto pfs_storage_service = simulation.add(new wrench::SimpleStorageService(pfs_storage_host, 10000000000000.0));

  // Construct a list of execution hosts (i.e., compute node)
  std::set<std::string> execution_hosts = {};

  for (auto host : hostname_list) {
    simgrid::s4u::Host* simhost = simgrid::s4u::Host::by_name(host); 
    if(std::string(simhost->get_property("type")) == std::string(COMPUTE_NODE))
      execution_hosts.insert(host);
  }

  if (execution_hosts.empty()) {
    throw std::runtime_error("This simulation requires at least one compute node in the platform file");
  }
  // Create a list of storage services that will be used by the WMS
  std::set<std::shared_ptr<wrench::StorageService>> storage_services;
  storage_services.insert(pfs_storage_service);

  // Create a list of compute services that will be used by the WMS
  std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

  std::string wms_host = PFS_HOST;
  // Instantiate a bare metal service and add it to the simulation
  try {
    auto baremetal_service = new wrench::BareMetalComputeService(
          wms_host, execution_hosts, 0, {}, {});

    compute_services.insert(simulation.add(baremetal_service));
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  // Instantiate a WMS
  auto wms = simulation.add(
          new BBWMS(
            std::unique_ptr<BBJobScheduler>(new BBJobScheduler(pfs_storage_service)),
            nullptr, compute_services, storage_services, wms_host)
          );
  wms->addWorkflow(workflow);

  // Instantiate a file registry service
  std::string file_registry_service_host = PFS_HOST;
  auto file_registry_service =
          new wrench::FileRegistryService(file_registry_service_host);
  simulation.add(file_registry_service);

  // It is necessary to store, or "stage", input files
  auto input_files = workflow->getInputFiles();
  try {
    simulation.stageFiles(input_files, pfs_storage_service);
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  // Launch the simulation
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  return 0;
}

