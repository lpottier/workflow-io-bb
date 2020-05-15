#!/bin/bash

DIR="$1"

cd $DIR

for f in *; do
    if [ -d "$f" ]; then
        #swarp-run-1N-0F.A0cK0h/swarp-1.batch.32c.0f.97928/1/0/stat.resample.*.xml 
        cd "$f"
        for run in *; do
            if [ -d "$run" ]; then
                cd "$run"
                for last in *; do
                    if [ -d "$last" ]; then
                        echo "$f/$run/$last"
                        cd "$last"
                        for avg in *; do
                            if [ -d "$avg" ]; then
                                cd $avg
                                for pipeline in *; do
                                    if [ -d "$pipeline" ]; then
                                        cd "$pipeline"
                                        rsmpl=$(cat stat.resample*.xml | sed -n -e 's/^&gt; All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
                                        combine=$(cat stat.combine*.xml | sed -n -e 's/^&gt; All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
                                        echo "$run : $avg -> $rsmpl $combine"
                                        cd ..
                                    fi
                                done
                                cd ..
                            fi
                        done
                        cd ..
                    fi
                done
                cd ..
            fi
        done
        cd ..
    fi
done

cd ..
