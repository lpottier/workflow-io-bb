#!/bin/bash

usage()
{
    echo "usage: $0 [[[-f=file ] [-c=COUNT]] | [-h]]"
}

checkbb() {
    if [ -z "$DW_JOB_STRIPED" ]; then
	    echo "Error: burst buffer allocation found. Run start_nostage.sh first"
	    exit
    fi
}

PWD=$(pwd)
RUNDIR=$(pwd)
TOTAL_FILES=64 #64 files per pipeline
BB_FILES=0
BB=0
#SRUN="srun -N 1 -n 1 -c 1"

for i in "$@"; do
	case $i in
    		-r=*|--run=*)
    			AVG="${i#*=}"
    			shift # past argument=value
    		;;
    		-b|--bb)
                checkbb
    			RUNDIR=$DW_JOB_STRIPED/
                BB_FILES=64
                BB=1
    			shift # past argument=value
    			;;
    		-h|--usage)
    			usage
    			exit
    		;;
    		*)
          	# unknown option
	  		usage
			exit
    		;;
esac
done

if [ -z "$AVG" ]; then
    AVG=1
fi

if [ -z "$VERBOSE" ]; then
    VERBOSE=0
fi

OUTDIR="$RUNDIR/output"

if (( $BB == 1 )); then
    CSV="$OUTDIR/data-bb.csv"
else
    CSV="$OUTDIR/data.csv"
fi

mkdir -p $RUNDIR/resamp
mkdir -p $OUTDIR

input_rsmpl="$RUNDIR/input/PTF201111*.w.fits"
output_rsmpl="$OUTDIR/output_resample.log"

input_coadd="$RUNDIR/resamp/PTF201111*.w.resamp.fits"
output_coadd="$OUTDIR/output_coadd.log"

if (( $BB == 1 )); then
    config_rsmpl="$RUNDIR/config/resample-bb.swarp"
    config_coadd="$RUNDIR/config/combine-bb.swarp"
else
    config_rsmpl="$RUNDIR/config/resample.swarp"
    config_coadd="$RUNDIR/config/combine.swarp"
fi

rm -rf $RUNDIR/*.fits $RUNDIR/*.xml

SEP="===================================================================================================="
echo "$SEP"

echo "RUNDIR      -> $RUNDIR"
echo "OUTDIR      -> $OUTDIR"
echo "CSV         -> $CSV"
echo "AVG         -> $AVG"
echo "BB_FILES    -> $BB_FILES"
echo "TOTAL_FILES -> $TOTAL_FILES"
echo "SRUN        -> $SRUN"

echo "$SEP"

echo "RUN USEBB FILES FILESBB STAGEIN RSMPL COADD MAKESPAN" > $CSV

echo -e "\tRUN ID \tSTAGEIN (S) \t\tRSMPL (S) \t\tCOADD (S) \t\tTOTAL (S)"

all_stagein=()
all_rsmpl=()
all_coadd=()
all_total=()

for k in $(seq 1 1 $AVG); do
    USE_BB='N'
    time_stagein=0
    if (( $BB == 0 )); then
        if (( $VERBOSE >= 2 )); then
            echo "[$k] No stage in, using PFS."
        fi
    else
        if (( $VERBOSE >= 2 )); then
            echo "[$k] START stagein:$(date --rfc-3339=ns)"
        fi
        t1=$(date +%s.%N)
        cp swarp $RUNDIR
        cp -r input/ $RUNDIR/
        cp -r config/ $RUNDIR/
        t2=$(date +%s.%N)

        if (( $VERBOSE >= 2 )); then
            echo "[$k] END stagein:$(date --rfc-3339=ns)"
        fi
        BB_FILES=64
        USE_BB='Y'
        time_stagein=$(echo "$t2 - $t1" | bc -l)
    fi

    if (( $VERBOSE >= 2 )); then
        echo "[$k] START rsmpl:$(date --rfc-3339=ns)"
    fi
    
    $SRUN $RUNDIR/swarp -c $config_rsmpl $input_rsmpl > $output_rsmpl 2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END rsmpl:$(date --rfc-3339=ns)"
    fi

    if (( $VERBOSE >= 2 )); then 
        echo "[$k] START coadd:$(date --rfc-3339=ns)"
    fi
    
    $SRUN $RUNDIR/swarp -c $config_coadd $input_coadd > $output_coadd  2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END coadd:$(date --rfc-3339=ns)"
    fi

    time_rsmpl=$(cat $output_rsmpl | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_coadd=$(cat $output_coadd | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_total=$(echo "$time_stagein + $time_rsmpl + $time_coadd" | bc -l)
    
    all_stagein+=( $time_stagein )
    all_rsmpl+=( $time_rsmpl )
    all_coadd+=( $time_coadd )
    all_total+=( $time_total )

    echo -e "\t$k \t$time_stagein \t\t\t$time_rsmpl \t\t\t$time_coadd \t\t\t$time_total "
    
    echo "$k $USE_BB $TOTAL_FILES $BB_FILES $time_stagein $time_rsmpl $time_coadd $time_total" >> $CSV
    
    rm -rf $RUNDIR/resamp/*

done


sum_stagein=0
for i in ${all_stagein[@]}; do
    sum_stagein=$(echo "$sum_stagein + $i" | bc -l)
done

sum_rsmpl=0
for i in ${all_rsmpl[@]}; do
    sum_rsmpl=$(echo "$sum_rsmpl + $i" | bc -l)
done

sum_coadd=0
for i in ${all_coadd[@]}; do
    sum_coadd=$(echo "$sum_coadd + $i" | bc -l)
done

sum_total=0
for i in ${all_total[@]}; do
    sum_total=$(echo "$sum_total + $i" | bc -l)
done

avg_stagein=$(echo "$sum_stagein / $AVG" | bc -l)
avg_rsmpl=$(echo "$sum_rsmpl / $AVG" | bc -l)
avg_coadd=$(echo "$sum_coadd / $AVG" | bc -l)
avg_total=$(echo "$sum_total / $AVG" | bc -l)

sd_stagein=0
sd_rsmpl=0
sd_coadd=0
sd_total=0

for i in ${all_stagein[@]}; do
    sd_stagein=$(echo "$sd_stagein + ($i - $avg_stagein)^2" | bc -l)
done

for i in ${all_rsmpl[@]}; do
    sd_rsmpl=$(echo "$sd_rsmpl + ($i - $avg_rsmpl)^2" | bc -l)
done

for i in ${all_coadd[@]}; do
    sd_coadd=$(echo "$sd_coadd + ($i - $avg_coadd)^2" | bc -l)
done

for i in ${all_total[@]}; do
    sd_total=$(echo "$sd_total + ($i - $avg_total)^2" | bc -l)
done

sd_stagein=$(echo "sqrt($sd_stagein / $AVG)" | bc -l)
sd_rsmpl=$(echo "sqrt($sd_rsmpl / $AVG)" | bc -l)
sd_coadd=$(echo "sqrt($sd_coadd / $AVG)" | bc -l)
sd_total=$(echo "sqrt($sd_total / $AVG)" | bc -l)

echo ""

printf "Avg: \t\t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \n" $avg_stagein $sd_stagein $avg_rsmpl $sd_rsmpl $avg_coadd $sd_coadd $avg_total $sd_total

echo "$SEP"

rm -rf *.{fits,xml} resamp

