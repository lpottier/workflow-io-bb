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

RUNDIR=$(pwd)
TOTAL_FILES=64 #64 files per pipeline
BB_FILES=0
BB=0
WRAPPER="srun -N 1 -n 1 -c 1"

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
config_rsmpl="$RUNDIR/config/resample.swarp"

input_coadd="$RUNDIR/resamp/PTF201111*.w.resamp.fits"
output_coadd="$OUTDIR/output_coadd.log"
config_coadd="$RUNDIR/config/combine.swarp"

rm -rf $RUNDIR/*.fits $RUNDIR/*.xml

SEP="===================================================================================================="
echo "$SEP"

echo "RUNDIR      -> $RUNDIR"
echo "OUTDIR      -> $OUTDIR"
echo "CSV         -> $CSV"
echo "AVG         -> $AVG"
echo "BB_FILES    -> $BB_FILES"
echo "TOTAL_FILES -> $TOTAL_FILES"

echo "$SEP"

echo "RUN USEBB FILES FILESBB RSMPL COADD MAKESPAN" > $CSV

echo -e "\tRUN ID \tRSMPL (S)\t\t\tCOADD (S)\t\t\tTOTAL (S)"

all_rsmpl=()
all_coadd=()
all_total=()

for k in $(seq 1 1 $AVG); do
    USE_BB='N'
    if (( $BB == 0 )); then
        if (( $VERBOSE >= 2 )); then
            echo "[$k] No stage in, using PFS."
        fi
    else
        if (( $VERBOSE >= 2 )); then
            echo "[$k] START stagein:$(date --rfc-3339=ns)"
        fi
        cp swarp $RUNDIR
        cp -r input/ $RUNDIR/
        cp -r config/ $RUNDIR/
        if (( $VERBOSE >= 2 )); then
            echo "[$k] END stagein:$(date --rfc-3339=ns)"
        fi
        BB_FILES=64
        USE_BB='Y'
    fi
    if (( $VERBOSE >= 2 )); then
        echo "[$k] START rsmpl:$(date --rfc-3339=ns)"
    fi
    
    $WRAPPER $RUNDIR/swarp -c $config_rsmpl $input_rsmpl > $output_rsmpl 2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END rsmpl:$(date --rfc-3339=ns)"
    fi

    if (( $VERBOSE >= 2 )); then 
        echo "[$k] START coadd:$(date --rfc-3339=ns)"
    fi
    
    $WRAPPER $RUNDIR/swarp -c $config_coadd $input_coadd > $output_coadd  2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END coadd:$(date --rfc-3339=ns)"
    fi

    time_rsmpl=$(cat $output_rsmpl | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_coadd=$(cat $output_coadd | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_total=$(echo "$time_rsmpl + $time_coadd" | bc -l)
    
    all_rsmpl+=( $time_rsmpl )
    all_coadd+=( $time_coadd )
    all_total+=( $time_total )

    echo -e "\t$k \t$time_rsmpl \t\t\t\t$time_coadd \t\t\t\t$time_total "
    
    echo "$k $USE_BB $TOTAL_FILES $BB_FILES $time_rsmpl $time_coadd $time_total" >> $CSV
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

avg_rsmpl=$(echo "$sum_rsmpl / $AVG" | bc -l)
avg_coadd=$(echo "$sum_coadd / $AVG" | bc -l)
avg_total=$(echo "$sum_total / $AVG" | bc -l)

sd_rsmpl=0
sd_coadd=0
sd_total=0
for i in $(seq 0 1 $AVG); do
    sd_rsmpl=$(echo "$sd_rsmpl + (${all_rsmpl[i]} - $avg_rsmpl)^2" | bc -l)
    sd_coadd=$(echo "$sd_coadd + (${all_coadd[i]} - $avg_coadd)^2" | bc -l)
    sd_total=$(echo "$sd_total + (${all_total[i]} - $avg_total)^2" | bc -l)
done

sd_rsmpl=$(echo "sqrt($sd_rsmpl / $AVG)" | bc -l)
sd_coadd=$(echo "sqrt($sd_coadd / $AVG)" | bc -l)
sd_total=$(echo "sqrt($sd_total / $AVG)" | bc -l)

echo ""

printf "Avg: \t\t%-0.2f (+/- %0.2f) \t\t%-0.2f (+/- %0.2f) \t\t%-0.2f (+/- %0.2f) \n" $avg_rsmpl $sd_rsmpl $avg_coadd $sd_coadd $avg_total $sd_total

echo "$SEP"

rm -rf *.{fits,xml} resamp

