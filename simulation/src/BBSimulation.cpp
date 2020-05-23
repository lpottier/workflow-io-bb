/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#include "BBSimulation.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(bb_simulation, "Log category for BB Simulation");

/**
 * @brief Public constructor
 *
 * @param hostname: the name of the host on which to start the service
 */
BBSimulation::BBSimulation(const std::string& jobid,
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
                           const std::string& output_dir) :
            wrench::Simulation() {

  this->simulation_job_id = jobid;
  this->simulation_id = id;
  this->nb_pipeline = pipeline;
  this->max_pipeline = max_pipeline;
  this->nb_cores = cores;
  
  this->bb_type = bb_type;
  for (auto & c: this->bb_type) c = std::toupper(c);

  if (fits == "1"){
    this->fits = "Y";
  }
  else {
    this->fits = "N";
  }
  this->measured_makespan = std::stod(makespan);
  raw_args["platform_file"] = platform_file;
  raw_args["workflow_file"] = workflow_file;
  raw_args["stage_list"] = stage_list;
  raw_args["real_log"] = real_log;
  raw_args["output_dir"] = output_dir;

  this->nb_files_in_bb = 0; // number of files in BB
  this->amount_of_data_in_bb = 0.0; // Amount of data in BB (Bytes)

  this->nb_files_staged = 0; // number of files staged
  this->amount_of_data_staged = 0.0; // Amount of data staged
}

void BBSimulation::init(int *argc, char **argv) {
  // Initialization of the simulation
  this->wrench::Simulation::init(argc, argv);

  //MAKE SURE THE FILE/DIR EXIST/HAS PERM

  // // Get a vector of all the hosts in the simulated platform
  //std::vector<std::string> hostname_list = this->getHostnameList();
  // std::set<std::string> hostname_set(hostname_list.begin(), hostname_list.end());
}

std::map<std::string, std::shared_ptr<wrench::StorageService> >
BBSimulation::parseFilesList(std::string path, std::shared_ptr<wrench::StorageService> pfs_service, std::shared_ptr<wrench::StorageService> bb_service) {
  std::map<std::string, std::shared_ptr<wrench::StorageService> > files_to_stages;

  /// Parse stage_list file file_src file_dest
  std::string line;
  std::string delimiter = " ";
  std::string path_delimiter = "/\\";
  std::string bb_prefix_cori = "/var/opt/cray/dws/";
  std::string bb_prefix_summit = "/mnt/bb/";
  std::ifstream stage_list_file (path);

  if (stage_list_file.is_open()) {
    while ( std::getline (stage_list_file,line) ) {
      // Parse the line to separate the source from the destination
      std::string src_line = line.substr(0, line.find(delimiter));
      std::string dst_line = line.substr(line.find(delimiter)+delimiter.length(), line.length());

      // Parse the source to get the filename and the base path
      std::size_t src_name_pos = src_line.find_last_of(path_delimiter);
      std::string src_path = src_line.substr(0,src_name_pos);
      std::string src_file = src_line.substr(src_name_pos+1);
      // std::cout << " path src: " << src_path << '\n';
      // std::cout << " file src: " << src_file << '\n';

      // Parse the destination to get the filename and the base path
      std::size_t dst_name_pos = dst_line.find_last_of(path_delimiter);
      std::string dst_path = dst_line.substr(0,dst_name_pos);
      std::string dst_file = dst_line.substr(dst_name_pos+1);
      // std::cout << " path dst: " << dst_path << '\n';
      // std::cout << " file dst: " << dst_file << '\n';

      if (src_path.find_first_of(bb_prefix_cori) == std::string::npos || src_path.find_first_of(bb_prefix_summit) == std::string::npos) {
        std::cerr << "[ERROR] file: " << src_file << " cannot be in burst buffers before being staged in" << std::endl;
        std::exit(1);
      }

      if (src_file != dst_file && dst_file.length() > 0) {
        std::cerr << "[ERROR] file: " << src_file << " is different from destination file: " << dst_file << std::endl;
        std::exit(1);
      }

      if (dst_path.find_first_of(bb_prefix_cori) != std::string::npos || dst_path.find_first_of(bb_prefix_summit) != std::string::npos) {
        // Goes on the first (and only) BB nodes
        files_to_stages[src_file] = bb_service;
        this->nb_files_staged++;
      } 
      else {
        files_to_stages[src_file] = pfs_service;
      }
    }
    stage_list_file.close();
  }
  else {
    std::cerr << "Unable to open stage list:" << path << std::endl;
    std::exit(1);
  }

  return files_to_stages;
}

std::map<std::string, double> BBSimulation::parseRealWorkflowLog(std::string path) {
  std::string line;
  char delimiter = ' ';
  std::ifstream log_file (path);
  std::map<std::string, double> real_data;

  if (log_file.is_open()) {
    while (std::getline (log_file,line)) {
      // If there a space in the line and the first word of the line is "TIME" then parse it
      std::size_t first_delim = line.find_first_of(delimiter);
      if (first_delim != std::string::npos && line.substr(0,first_delim) == "TIME") {
        std::string second_part = line.substr(first_delim+1);
        // Parse the source to get the filename and the base path
        std::size_t second_delim = second_part.find_first_of(delimiter);

        real_data[second_part.substr(0,second_delim)] = std::stod(second_part.substr(second_delim+1));
      }
    }
    log_file.close();
  }
  else {
    std::cerr << "Unable to open workflow log: " << path << std::endl;
    std::exit(1);
  }

  return real_data;
}

wrench::Workflow* BBSimulation::parse_inputs() {
  std::size_t workflowf_pos = this->raw_args["workflow_file"].find_last_of("/");
  std::size_t platformf_pos = this->raw_args["platform_file"].find_last_of("/");

  this->workflow_id = this->raw_args["workflow_file"].substr(workflowf_pos+1);
  this->platform_id = this->raw_args["platform_file"].substr(platformf_pos+1);

  //MAKE SURE THE DIR EXIST/HAS PERM

  // Reading and parsing the platform description file to instantiate a simulated platform
  this->instantiatePlatform(this->raw_args["platform_file"]);

  /* Reading and parsing the workflow description file to create a wrench::Workflow object */

  /* The have a 1:1 accurate comparison, we need to set up the number of flop */
  std::string flop_rate = std::to_string(this->getHostFlopRate("Host1"));

  if (ends_with(this->raw_args["workflow_file"], "dax")) {
      this->workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(raw_args["workflow_file"], flop_rate+"f");
  } else if (ends_with(this->raw_args["workflow_file"],"json")) {
      this->workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(raw_args["workflow_file"], flop_rate+"f");
  } else {
      std::cerr << "Workflow file: " << raw_args["workflow_file"] << "name must end with '.dax' or '.json'" << std::endl;
      exit(1);
  }
  std::cerr << "The workflow has " << this->workflow->getNumberOfTasks() << " tasks " << std::endl;
  auto sizefiles = this->workflow->getFiles();

  double totsize = 0;
  for (auto f : sizefiles)
    totsize += f->getSize();
  std::cerr << "Total files size " << totsize << " Bytes (" << totsize/std::pow(2,40) << " TB)" << std::endl;  
  std::cerr.flush();

  // Parse real data to compare with simulated makespan
  if (this->raw_args["real_log"] != "0")
    this->real_data_run = parseRealWorkflowLog(this->raw_args["real_log"]);
  else
    this->real_data_run["TOTAL"] = 0;

  return this->workflow;
}

std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*>> 
BBSimulation::create_hosts() {
  std::vector<std::string> hostname_list = this->getHostnameList();

  int pfs_count = 0;
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

      this->hostpair_to_link[std::make_pair(host,dest)] = route;

      // If the route is going from a compute node to a storage node
      // route.size() == 1 is here to ensure that we consider a route with only one hop between CN and Storage
      // It is a requirement in the private BB case (Summit)
      if (host_type == std::string(COMPUTE_NODE) && 
          host_dest_type == std::string(STORAGE_NODE) &&
          route.size() == 1) {
        std::string host_dest_category = std::string(host_dest->get_property("category"));
        
        if (host_dest_category == std::string(PFS_NODE)) {
          this->cs_to_pfs[std::make_pair(host,dest)] = route[0];
        }
        else if (host_dest_category == std::string(BB_NODE)) {
          this->cs_to_bb[std::make_pair(host,dest)] = route[0];
        }
      }

      route.clear();
    }

    if (host_type == std::string(COMPUTE_NODE)) {
      this->execution_hosts.insert(host);
    }
    else if (host_type == std::string(STORAGE_NODE)) {
      std::string size = std::string(simhost->get_property("size"));
      std::string category = std::string(simhost->get_property("category"));

      if (category == std::string(PFS_NODE)) {
        pfs_count++;
        this->pfs_storage_host = std::make_tuple(host, std::stod(size));
      }
      else if (category == std::string(BB_NODE)) {
        this->bb_storage_hosts.insert(std::make_tuple(host, std::stod(size)));
      }
    }
  }

  if (pfs_count != 1)
    throw std::runtime_error("This simulation requires exactly one PFS host");

  if (this->execution_hosts.empty())
    throw std::runtime_error("This simulation requires at least one compute node in the platform file");

  if (this->bb_storage_hosts.empty())
    throw std::runtime_error("This simulation requires at least one burst buffer node in the platform file");

  return this->hostpair_to_link;
}


//route: map (host_src, host_dest) -> Link
std::pair<double, double> BBSimulation::check_links(std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*> route) {
  std::pair<double, double> res = std::make_pair(0.0, 0.0);

  auto first_elem = *route.begin();

  res.first = first_elem.second->get_bandwidth();
  res.second = first_elem.second->get_latency();

  for (auto elem : route) {
    // std::cout << "CS: " << elem.first.first << " attached to PFS:" << elem.first.second 
    //           << " bandwidth:" << elem.second->get_bandwidth()
    //           << " latency:" << elem.second->get_latency() 
    //           << std::endl;

    if (res.first != elem.second->get_bandwidth()) {
      std::cerr << "Bandwidth issue in the platform XML. The same bandwidth must be used for every similar links." << std::endl;
      exit(1);
    }

    if (res.second != elem.second->get_latency()) {
      std::cerr << "Latency issue in the platform XML. The same latency must be used for every similar links." << std::endl;
      exit(1);
    }

  }
  return res;
}

std::set<std::shared_ptr<wrench::StorageService>> 
BBSimulation::instantiate_storage_services() {
  // Create a list of storage services that will be used by the WMS
  
  this->pfs_link = this->check_links(this->cs_to_pfs);
  this->bb_link = this->check_links(this->cs_to_bb);

  std::map<std::string, double> pfs_payload = {
      {wrench::SimpleStorageServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD,              0},
      {wrench::SimpleStorageServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD,           0},
      {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_REQUEST_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_ANSWER_MESSAGE_PAYLOAD,        0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_REQUEST_MESSAGE_PAYLOAD,      0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_ANSWER_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_REQUEST_MESSAGE_PAYLOAD,      0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_ANSWER_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_REQUEST_MESSAGE_PAYLOAD,        0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD,         0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD,       0}, //latest res: 100000
      {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_ANSWER_MESSAGE_PAYLOAD,        0}, //latest res: 100000
      {wrench::SimpleStorageServiceMessagePayload::FILE_READ_REQUEST_MESSAGE_PAYLOAD,        0}, //latest res: 100000
      {wrench::SimpleStorageServiceMessagePayload::FILE_READ_ANSWER_MESSAGE_PAYLOAD,         0}, //latest res: 100000
  };

  std::map<std::string, double> bb_payload = {
      {wrench::SimpleStorageServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD,              0},
      {wrench::SimpleStorageServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD,           0},
      {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_REQUEST_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_ANSWER_MESSAGE_PAYLOAD,        0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_REQUEST_MESSAGE_PAYLOAD,      0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_ANSWER_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_REQUEST_MESSAGE_PAYLOAD,      0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_ANSWER_MESSAGE_PAYLOAD,       0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_REQUEST_MESSAGE_PAYLOAD,        0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD,         0},
      {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD,       80000000}, // latest res: 120000000
      {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_ANSWER_MESSAGE_PAYLOAD,        80000000}, // latest res: 120000000
      {wrench::SimpleStorageServiceMessagePayload::FILE_READ_REQUEST_MESSAGE_PAYLOAD,        80000000}, // latest res: 120000000
      {wrench::SimpleStorageServiceMessagePayload::FILE_READ_ANSWER_MESSAGE_PAYLOAD,         80000000}, // latest res: 120000000
  };

  try {
    this->pfs_storage_service = this->add(new PFSStorageService(
                                              std::get<0>(this->pfs_storage_host), 
                                              std::get<1>(this->pfs_storage_host),
                                              this->pfs_link.first,
                                              this->pfs_link.second,
                                              {},
                                              {},
                                              pfs_payload)
                                      );
    this->storage_services.insert(this->pfs_storage_service);
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  try {
    for (auto bbhost : this->bb_storage_hosts) {
      auto service = this->add(new BBStorageService(
                                    std::get<0>(bbhost),
                                    std::get<1>(bbhost),
                                    this->bb_link.first,
                                    this->bb_link.second,
                                    this->pfs_storage_service,
                                    {},
                                    {},
                                    bb_payload)
                              );
      this->storage_services.insert(service);
      this->bb_storage_services.insert(service);
    }
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  return storage_services;
}

std::set<std::shared_ptr<wrench::ComputeService>> BBSimulation::instantiate_compute_services() {

  // Create a list of compute services that will be used by the WMS
  // Instantiate a bare metal service and add it to the simulation

  std::map< std::string, std::tuple< unsigned long, double >> compute_resources;
  // for (auto host : this->execution_hosts) {
  //   compute_resources[host] = std::make_tuple(wrench::ComputeService::ALL_CORES, wrench::ComputeService::ALL_RAM);
  // }

  std::map<std::string, double> compute_payload_values = {
      {wrench::ComputeServiceMessagePayload::JOB_TYPE_NOT_SUPPORTED_MESSAGE_PAYLOAD,         0}, 
      {wrench::ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD,    1500000000}, //latest res: 3000000000
      {wrench::ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,     0},
      {wrench::ComputeServiceMessagePayload::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,              0},
      {wrench::ComputeServiceMessagePayload::STANDARD_JOB_FAILED_MESSAGE_PAYLOAD,            0},
      {wrench::ComputeServiceMessagePayload::TERMINATE_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, 0},
      {wrench::ComputeServiceMessagePayload::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  0},
      {wrench::ComputeServiceMessagePayload::SUBMIT_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,       0},
      {wrench::ComputeServiceMessagePayload::SUBMIT_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,        0},
      {wrench::ComputeServiceMessagePayload::PILOT_JOB_STARTED_MESSAGE_PAYLOAD,              0},
      {wrench::ComputeServiceMessagePayload::PILOT_JOB_EXPIRED_MESSAGE_PAYLOAD,              0},
      {wrench::ComputeServiceMessagePayload::PILOT_JOB_FAILED_MESSAGE_PAYLOAD,               0},
      {wrench::ComputeServiceMessagePayload::TTL_REQUEST_MESSAGE_PAYLOAD,                    0},
      {wrench::ComputeServiceMessagePayload::TTL_ANSWER_MESSAGE_PAYLOAD,                     0},
      {wrench::ComputeServiceMessagePayload::TERMINATE_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,    0},
      {wrench::ComputeServiceMessagePayload::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,     0},
      {wrench::ComputeServiceMessagePayload::RESOURCE_DESCRIPTION_REQUEST_MESSAGE_PAYLOAD,   0},
      {wrench::ComputeServiceMessagePayload::RESOURCE_DESCRIPTION_ANSWER_MESSAGE_PAYLOAD,    0},
      {wrench::BareMetalComputeServiceProperty::THREAD_STARTUP_OVERHEAD,                     0},
  };

  try {
    // auto baremetal_service = new wrench::BareMetalComputeService(
    //       std::get<0>(this->pfs_storage_host), this->execution_hosts, 0, {}, {});

    for (auto host : this->execution_hosts) {
      compute_resources[host] = std::make_tuple(wrench::ComputeService::ALL_CORES, wrench::ComputeService::ALL_RAM);
      
      auto baremetal_service = new wrench::BareMetalComputeService(
            host, compute_resources, 0, {}, compute_payload_values);

      this->compute_services.insert(this->add(baremetal_service));
    }
  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  return compute_services;
}

wrench::FileRegistryService* BBSimulation::instantiate_file_registry_service() {

  //All services run on the main PFS node (by rule PFSHost1)
  // Instantiate a file registry service
  try {
    this->file_registry_service = new wrench::FileRegistryService(std::get<0>(this->pfs_storage_host));
    this->add(file_registry_service);

  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  return file_registry_service;
}

std::pair<int, double> BBSimulation::stage_input_files() {
  // It is necessary to store, or "stage", input files in the PFS
  auto input_files = this->workflow->getInputFiles();
  try {
    this->stageFiles(input_files, this->pfs_storage_service);
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    std::exit(1);
  }

  double result = std::accumulate(
                      std::begin(input_files), std::end(input_files), 0,
                      [](double previous, std::pair<std::string, wrench::WorkflowFile*> elem) 
                                        { return previous + elem.second->getSize(); }
                                );

  return std::make_pair(input_files.size(), result);
}

std::shared_ptr<wrench::WMS> BBSimulation::instantiate_wms_service(const FileMap_t& file_placement_heuristic) {
  // Instantiate a WMS
  this->wms = this->add(
          new BBWMS(
            std::unique_ptr<BBJobScheduler>(new BBJobScheduler(file_placement_heuristic, this->nb_cores)),
            nullptr, this->compute_services, 
            this->storage_services,
            std::get<0>(this->pfs_storage_host))
          );
  this->wms->addWorkflow(this->workflow);
  return this->wms;
}

void BBSimulation::dumpWorkflowStatCSV() {
  printWorkflowFile(this->workflow_id, 
                    this->workflow, 
                    this->raw_args["output_dir"] + "/" + this->workflow_id + "-stat.csv");
}


void BBSimulation::dumpAllOutputJSON(bool layout) {
  auto& simulation_output = this->getOutput();

  simulation_output.dumpPlatformGraphJSON(this->raw_args["output_dir"] + "/" + this->platform_id + ".json");
  simulation_output.dumpWorkflowExecutionJSON(this->workflow, this->raw_args["output_dir"] + "/" + this->workflow_id + "-execution.json", false);

  if (layout)
    simulation_output.dumpWorkflowExecutionJSON(this->workflow, this->raw_args["output_dir"] + "/" + this->workflow_id + "-execution-layout.json", true);

  simulation_output.dumpWorkflowGraphJSON(this->workflow, this->raw_args["output_dir"] + "/" + this->workflow_id + ".json");
}

void BBSimulation::dumpResultPerTaskCSV(const std::string& output, char sep) {
  auto& simulation_output = this->getOutput();

  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks;
  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampFileCopyCompletion> *> trace_copyfiles;

  trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();
  trace_copyfiles = simulation_output.getTrace<wrench::SimulationTimestampFileCopyCompletion>();

  auto stage_in = trace_tasks[0]->getContent()->getTask();
  auto stage_out = trace_tasks[trace_tasks.size()-1]->getContent()->getTask();

  std::cout << "Task in first trace entry: " << stage_in->getID() << " " << stage_in->getStartDate() << " " << stage_in->getEndDate() << " " << stage_in->getEndDate()-stage_in->getStartDate() << std::endl;
  std::cout << "Task in last trace entry: " << stage_out->getID() << " " << stage_out->getStartDate() << " " << stage_out->getEndDate() << " " << stage_out->getEndDate() - stage_out->getStartDate() << std::endl;
}
