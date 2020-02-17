/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_BBTYPES_H
#define MY_BBTYPES_H

#include <wrench-dev.h>

#include <limits>

typedef std::numeric_limits< double > dbl;

typedef std::set<std::tuple<wrench::WorkflowFile*, 
         std::shared_ptr<wrench::StorageService>, 
         std::shared_ptr<wrench::StorageService> > > FileMap_t;

enum STORAGE_TYPE {
    PFS,
    BB
};


// struct FileAllocation {
//     wrench::WorkflowFile* file,
//     std::shared_ptr<wrench::StorageService> src,
//     std::shared_ptr<wrench::StorageService> dst,
// };

#endif //MY_BBTYPES_H

