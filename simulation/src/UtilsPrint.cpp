/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>
#include <iomanip>
#include <fstream>

#include "UtilsPrint.h"
#include "BBSimulation.h"

bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}

void printWorkflowTTY(const std::string& workflow_id, 
                      wrench::Workflow* workflow) {

  auto precision = std::cout.precision();
  std::cout.precision(dbl::max_digits10);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  auto files = workflow->getFiles();
  auto tasks = workflow->getTasks();

  double totsize = 0;
  for (auto f : files)
    totsize += f->getSize();

  double avg_size = totsize/files.size();
  double sd_size = 0.0;

  for (auto f : files)
    sd_size += std::pow(f->getSize() - avg_size, 2);

  sd_size = std::sqrt(sd_size/files.size());

  double avg_flop = workflow->getSumFlops(tasks)/tasks.size();
  double sd_flops = 0.0;

  for (auto t : tasks)
    sd_flops += std::pow(t->getFlops() - avg_flop, 2);

  sd_flops = std::sqrt(sd_flops/tasks.size());

  std::cout << std::left << std::setw(20) << " WORKFLOW " << std::left << std::setw(30) << workflow_id << std::endl;
  std::cout << std::left << std::setw(20) << " TASKS " << std::left << std::setw(30) << workflow->getNumberOfTasks() << std::endl;
  std::cout << std::left << std::setw(20) << " LEVELS " << std::left << std::setw(30) << workflow->getNumLevels() << std::endl;
  std::cout << std::left << std::setw(20) << " OPTOT(FLOP) " << std::left << std::setw(30) << workflow->getSumFlops(tasks) << std::endl;
  std::cout << std::left << std::setw(20) << " OPAVG(FLOP) " << std::left << std::setw(30) << avg_flop << std::endl;
  std::cout << std::left << std::setw(20) << " OPSD(FLOP) " << std::left << std::setw(30) << sd_flops << std::endl;
  std::cout << std::left << std::setw(20) << " OPSD(%) " << std::left << std::setw(30) << sd_flops*100/avg_flop << std::endl;
  std::cout << std::left << std::setw(20) << " FILES " << std::left << std::setw(30) << files.size() << std::endl;
  std::cout << std::left << std::setw(20) << " SIZETOT(B) " << std::left << std::setw(30) << totsize << std::endl;
  std::cout << std::left << std::setw(20) << " SIZEAVG(B) " << std::left << std::setw(30) << avg_size << std::endl;
  std::cout << std::left << std::setw(20) << " SIZESD(B) " << std::left << std::setw(30) << sd_size << std::endl;
  std::cout << std::left << std::setw(20) << " SIZESD(%) " << std::left << std::setw(30) << sd_size*100/avg_size << std::endl;

  std::cout.flush();

  //back to previous precision
  std::cout.precision(precision);

}

void printWorkflowFile(const std::string& workflow_id, 
                       wrench::Workflow* workflow, 
                       const std::string& output,
                       const char sep) {

  std::ofstream ofs;
  ofs.open(output, std::ofstream::out | std::ofstream::trunc);

  auto files = workflow->getFiles();
  auto tasks = workflow->getTasks();

  double totsize = 0;
  for (auto f : files)
    totsize += f->getSize();

  double avg_size = totsize/files.size();
  double sd_size = 0.0;

  for (auto f : files)
    sd_size += std::pow(f->getSize() - avg_size, 2);

  sd_size = std::sqrt(sd_size/files.size());

  double avg_flop = workflow->getSumFlops(tasks)/tasks.size();
  double sd_flops = 0.0;

  for (auto t : tasks)
    sd_flops += std::pow(t->getFlops() - avg_flop, 2);

  sd_flops = std::sqrt(sd_flops/tasks.size());

  ofs << "WORKFLOW" << sep << workflow_id << std::endl;
  ofs << "TASKS" << sep << workflow->getNumberOfTasks() << std::endl;
  ofs << "LEVELS" << sep << workflow->getNumLevels() << std::endl;
  ofs << "OPTOT(FLOP)" << sep << workflow->getSumFlops(tasks) << std::endl;
  ofs << "OPAVG(FLOP)" << sep << avg_flop << std::endl;
  ofs << "OPSD(FLOP)" << sep << sd_flops << std::endl;
  ofs << "OPSD(%)" << sep << sd_flops*100/avg_flop << std::endl;
  ofs << "FILES" << sep << files.size() << std::endl;
  ofs << "SIZETOT(B)" << sep << totsize << std::endl;
  ofs << "SIZEAVG(B)" << sep << avg_size << std::endl;
  ofs << "SIZESD(B)" << sep << sd_size << std::endl;
  ofs << "SIZESD(%)" << sep << sd_size*100/avg_size << std::endl;

  ofs.close();

}

void printFileAllocationTTY(const FileMap_t& file_placement) {

  auto precision = std::cout.precision();
  std::cout.precision(dbl::max_digits10);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  int width_name = 50;
  int width_src = 15;
  int width_dest = 15;
  int width_size = 20;

  std::string sep_name(width_name-1, '-');
  std::string sep_src(width_src-1, '-');
  std::string sep_dest(width_dest-1, '-');
  std::string sep_size(width_size, '-');


  std::cout << std::left << std::setw(width_name+1) 
            << " FILE"
            << std::left << std::setw(width_src)
            << "SRC"
            << std::left << std::setw(width_dest)
            << "DEST"
            << std::left << std::setw(width_size) 
            << "SIZE(MB)" << std::endl;
  std::cout << std::left 
            << " " << sep_name << " "
            << std::left
            << sep_src << " "
            << std::left
            << sep_dest << " "
            << std::left
            << sep_size << std::endl;

  for (auto alloc : file_placement) {
    std::cout << " " << std::left << std::setw(width_name) 
              << std::get<0>(alloc)->getID()
              << std::left << std::setw(width_src)
              << std::get<1>(alloc)->getHostname()
              << std::left << std::setw(width_dest)
              << std::get<2>(alloc)->getHostname()
              << std::left << std::setw(width_size) 
              << std::get<0>(alloc)->getSize()/std::pow(2,20) << std::endl;
  }
  std::cout.flush();
  //back to previous precision
  std::cout.precision(precision);
}

void printSimulationSummaryTTY(BBSimulation& simulation, bool header) {

  auto& simulation_output = simulation.getOutput();

  auto precision = std::cout.precision();
  std::cout.precision(7);
  //std::cout.setf( std::ios::fixed, std:: ios::floatfield );

  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();

  double makespan = trace_tasks[0]->getDate();
  for (auto task : trace_tasks)
    makespan = makespan < task->getDate() ? task->getDate() : makespan;

  // Number of space to offset to the right
  int offset = 1;
  std::string off_str(offset, ' ');

  int width_jobid = 10;
  int width_id = 7;
  int width_pipeline = 12;
  int width_cores = 6;
  int width_fits = 5;
  int width_bbtype = 8;
  int width_workflow = 15;
  int width_platform = 15;
  int width_latency = 10;
  int width_link = 10;
  int width_bbfile = 10;
  int width_bbdata = 10;
  int width_mksp = 15;
  int width_walltime = 15;
  int width_err = 15;

  double latency_ratio = 0.0;
  double bandwith_ratio = 0.0;
  // TODO: fix this with epsilon interval
  if (simulation.getLatencyPFS() > 0.0) {
    latency_ratio = simulation.getLatencyBB()/simulation.getLatencyPFS();
  } 
  else {
    latency_ratio = simulation.getLatencyBB();
  }

  // TODO: fix this with epsilon interval
  if (simulation.getBandwithPFS() > 0.0) {
    bandwith_ratio = simulation.getBandwithBB()/simulation.getBandwithPFS();
  } 
  else {
    bandwith_ratio = simulation.getBandwithBB();
  }

  double err = abs(makespan - simulation.getMeasuredMakespan())/simulation.getMeasuredMakespan();

  if (header) {
    std::cout << std::left << std::setw(width_jobid+offset) 
              << off_str + "ID"
              << std::left << std::setw(width_id) 
              << "AVG"
              << std::left << std::setw(width_workflow) 
              << "WORKFLOW"
              << std::left << std::setw(width_platform)
              << "PLATFORM"
              << std::left << std::setw(width_fits)
              << "FITS"
              << std::left << std::setw(width_bbtype)
              << "BBTYPE"
              << std::left << std::setw(width_pipeline) 
              << "#PIPELINES"
              << std::left << std::setw(width_cores) 
              << "CORES"
              << std::left << std::setw(width_latency)
              << "FILES"
              << std::left << std::setw(width_bbfile) 
              << "DATA(MB)"
              << std::left << std::setw(width_bbdata) 
              << "LATENCY"
              << std::left << std::setw(width_link) 
              << "BANDWIDTH"
              << std::left << std::setw(width_mksp) 
              << "SIMULATION(S)"
              << std::left << std::setw(width_walltime) 
              << "MEASURED(S)"
              << std::left << std::setw(width_err) 
              << "ERR(%)" << std::endl;

    std::cout << std::left << std::setw(width_jobid+offset) 
              << off_str + "--"
              << std::left << std::setw(width_id) 
              << "---"
              << std::left << std::setw(width_workflow) 
              << "--------"
              << std::left << std::setw(width_platform)
              << "--------"
              << std::left << std::setw(width_fits)
              << "----"
              << std::left << std::setw(width_bbtype)
              << "------"
              << std::left << std::setw(width_pipeline) 
              << "----------"
              << std::left << std::setw(width_cores) 
              << "-----"
              << std::left << std::setw(width_bbfile) 
              << "-----"
              << std::left << std::setw(width_bbdata) 
              << "--------"
              << std::left << std::setw(width_latency) 
              << "-------"
              << std::left << std::setw(width_link) 
              << "---------"
              << std::left << std::setw(width_mksp) 
              << "-------------"
              << std::left << std::setw(width_walltime) 
              << "-----------"
              << std::left << std::setw(width_err) 
              << "------" << std::endl;
  }

  std::string pip = simulation.getNumberPipeline() + '/' + simulation.getMaxPipeline();

  std::cout << std::left << std::setw(width_jobid+offset)
            << off_str + simulation.getJobID()
            << std::left << std::setw(width_id) 
            << simulation.getID()
            << std::left << std::setw(width_workflow) 
            << simulation.getWorkflowID()
            << std::left << std::setw(width_platform)
            << simulation.getPlatformID()
            << std::left << std::setw(width_fits) 
            << simulation.getFITS()
            << std::left << std::setw(width_bbtype) 
            << simulation.getBBtype()
            << std::left << std::setw(width_pipeline) 
            << pip
            << std::left << std::setw(width_cores) 
            << simulation.getNumberCores()
            << std::left << std::setw(width_bbfile) 
            << simulation.getNBFileStaged()
            << std::left << std::setw(width_bbdata) 
            << simulation.getDataStaged()/std::pow(2,20)
            << std::left << std::setw(width_latency) 
            << latency_ratio
            << std::left << std::setw(width_link) 
            << bandwith_ratio
            << std::left << std::setw(width_mksp) 
            << makespan
            << std::left << std::setw(width_walltime) 
            << simulation.getMeasuredMakespan()
            << std::left << std::setw(width_err) 
            << err * 100 << std::endl;
    //back to previous precision
    std::cout.precision(precision);
}

void printSimulationSummaryCSV(BBSimulation& simulation, std::string file_name, bool header) {

  auto& simulation_output = simulation.getOutput();

  std::string sep = " ";

  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace_tasks = simulation_output.getTrace<wrench::SimulationTimestampTaskCompletion>();

  double makespan = trace_tasks[0]->getDate();
  for (auto task : trace_tasks)
    makespan = makespan < task->getDate() ? task->getDate() : makespan;

  double latency_ratio = 0.0;
  double bandwith_ratio = 0.0;
  // TODO: fix this with epsilon interval
  if (simulation.getLatencyPFS() > 0.0) {
    latency_ratio = simulation.getLatencyBB()/simulation.getLatencyPFS();
  } 
  else {
    latency_ratio = simulation.getLatencyBB();
  }

  // TODO: fix this with epsilon interval
  if (simulation.getBandwithPFS() > 0.0) {
    bandwith_ratio = simulation.getBandwithBB()/simulation.getBandwithPFS();
  } 
  else {
    bandwith_ratio = simulation.getBandwithBB();
  }

  double err = abs(makespan - simulation.getMeasuredMakespan())/simulation.getMeasuredMakespan();

  double err_walltime = abs(makespan - simulation.getRealWallTime())/simulation.getRealWallTime();


  std::fstream fs;
  if (header) {
    fs.open (file_name, std::fstream::out | std::fstream::trunc);
  }
  else {
    fs.open (file_name, std::fstream::out | std::fstream::app);
  }
  

  if (fs) {
    if (header) {
      fs << "ID"
         << sep
         << "AVG"
         << sep
         << "FITS"
         << sep
         << "BB_TYPE"
         << sep
         << "WORKFLOW"
         << sep
         << "PLATFORM"
         << sep
         << "NB_PIPELINE"
         << sep
         << "PIPELINE"
         << sep
         << "NB_CORES"
         << sep
         << "BB_NB_FILES"
         << sep
         << "DATA_MB"
         << sep
         << "LATENCY"
         << sep
         << "BANDWIDTH"
         << sep
         << "SIMULATION_S"
         << sep
         << "MEASURED_MKSP_S"
         << sep
         << "ERR_MKSP"
         << sep
         << "MEASURED_WALLTIME_S"
         << sep
         << "ERR_WALLTIME" 
         << std::endl;
    }

    fs << simulation.getJobID()
       << sep
       << simulation.getID()
       << sep
       << simulation.getFITS()
       << sep
       << simulation.getBBtype()
       << sep
       << simulation.getWorkflowID()
       << sep
       << simulation.getPlatformID()
       << sep
       << simulation.getMaxPipeline()
       << sep 
       << simulation.getNumberPipeline()
       << sep 
       << simulation.getNumberCores()
       << sep 
       << simulation.getNBFileStaged()
       << sep
       << simulation.getDataStaged()/std::pow(2,20)
       << sep 
       << latency_ratio
       << sep 
       << bandwith_ratio
       << sep
       << makespan
       << sep 
       << simulation.getMeasuredMakespan()
       << sep 
       << err
       << sep
       << simulation.getRealWallTime()
       << sep 
       << err_walltime
       << std::endl;
  }

  fs.close();

}

void printHostRouteTTY(const std::map<std::pair<std::string, std::string>, std::vector<simgrid::s4u::Link*> >& hostpair_to_link) {

  for (auto hostpair: hostpair_to_link) {
    std::cout << "Links from " << hostpair.first.first << " to " << hostpair.first.second << std::endl;
    for (auto link : hostpair.second) {
        std::cout << " | " << link->get_name() 
                  << " at speed " << link->get_bandwidth() 
                  << " latency: " << link->get_latency() << std::endl;
    }
  }

}

void printHostStorageAssociationTTY(const std::map<std::pair<std::string, std::string>, simgrid::s4u::Link*>& map) {

  int width_compute = 30;
  int width_storage = 20;
  int width_link = 20;
  int width_latency = 20;

  std::cout << std::left << std::setw(width_compute+1) 
            << " COMPUTE HOST"
            << std::left << std::setw(width_storage)
            << "STORAGE"
            << std::left << std::setw(width_link) 
            << "LINK(GB/s)"
            << std::left << std::setw(width_latency) 
            << "LATENCY(uS)" << std::endl;
  std::cout << std::left << std::setw(width_compute+1) 
            << " ------------"
            << std::left << std::setw(width_storage)
            << "-------"
            << std::left << std::setw(width_link) 
            << "----------"
            << std::left << std::setw(width_latency) 
            << "-----------" << std::endl;

  for (auto pair : map) {
    std::cout << " " << std::left << std::setw(width_compute) 
              << pair.first.first
              << std::left << std::setw(width_storage)
              << pair.first.second
              << std::left << std::setw(width_link) 
              << pair.second->get_bandwidth()
              << std::left << std::setw(width_latency) 
              << pair.second->get_latency() << std::endl;
  }
}

