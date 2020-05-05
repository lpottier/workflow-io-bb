#!/bin/bash

usage()
{
    echo "usage: $0 [[[-r=num of runs] [-j=number of tasks]  [-b=private | striped]] | [-h]]"
}

checkbb_striped() {
    if [ -z "$DW_JOB_STRIPED" ]; then
	    echo "Error: burst buffer allocation found. Run start.sh first"
	    exit
    fi
}

checkbb_private() {
    if [ -z "$DW_JOB_PRIVATE" ]; then
	    echo "Error: burst buffer allocation found. Run start-private.sh first"
	    exit
    fi
}

PWD=$(pwd)
RUNDIR=$(pwd)
TOTAL_FILES=64 #64 files per pipeline
BB_FILES=0
BB=0

for i in "$@"; do
	case $i in
    		-r=*|--run=*)
    			AVG="${i#*=}"
    			shift # past argument=value
    		;;
    		-t=*|--tasks=*)
    			NTASKS="${i#*=}"
    			shift # past argument=value
    		;;
    		-b=*|--bb=*)
                BBTYPE="${i#*=}"
                if [[ "$BBTYPE" == "striped" ]]; then
                    checkbb_striped
    			    RUNDIR=$DW_JOB_STRIPED/
                    BB=1
                elif [[ "$BBTYPE" == "private" ]]; then 
                    checkbb_private
    			    RUNDIR=$DW_JOB_PRIVATE/
                    BB=2
                else
                    echo "Error: must be either striped or private"
                    usage
                    exit
                fi
                BB_FILES=64
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

if [ -z "$NTASKS" ]; then
    NTASKS=1
fi

SRUN="srun -N 1 -n $NTASKS -l"

OUTDIR="$RUNDIR/output"

if (( $BB == 1 )); then
    CSV="$OUTDIR/data-bb.csv"
    SUMMARY_CSV="$OUTDIR/summary-bb.csv"
elif (( $BB == 2 )); then
    CSV="$OUTDIR/data-bb-priv.csv"
    SUMMARY_CSV="$OUTDIR/summary-bb-priv.csv"
else
    CSV="$OUTDIR/data.csv"
    SUMMARY_CSV="$OUTDIR/summary.csv"
fi

mkdir -p $RUNDIR/resamp
mkdir -p $OUTDIR
#lfs setstripe -c 1 -S 2048m $RUNDIR/resamp

input_rsmpl="$RUNDIR/input/PTF201111*.w.fits"
output_rsmpl="$OUTDIR/output_resample.log"

input_coadd="$RUNDIR/resamp/PTF201111*.w.resamp.fits"
output_coadd="$OUTDIR/output_coadd.log"

if (( $BB == 1 )); then
    config_rsmpl="$RUNDIR/config/resample-bb.swarp"
    config_coadd="$RUNDIR/config/combine-bb.swarp"
elif (( $BB == 2 )); then
    config_rsmpl="$RUNDIR/config/resample-bb-priv.swarp"
    config_coadd="$RUNDIR/config/combine-bb-priv.swarp"
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
echo "SUMMARY_CSV -> $SUMMARY_CSV"
echo "AVG         -> $AVG"
echo "BB_FILES    -> $BB_FILES"
echo "TOTAL_FILES -> $TOTAL_FILES"
echo "NTASKS      -> $NTASKS"
echo "SRUN        -> $SRUN"

echo "$SEP"

echo "RUN USEBB FILES FILESBB STAGEIN RSMPL COADD MAKESPAN" > $CSV
echo "RUN USEBB FILES FILESBB STAGEIN STAGE_SD RSMPL RSNPL_SD COADD COADD_SD MAKESPAN MAKESPAN_SD" > $SUMMARY_CSV

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
    
    echo "$k $USE_BB $TOTAL_FILES $BB_FILES $time_rsmpl $time_coadd $time_total" >> $CSV
    
    #if (( $BB == 1 )); then
    #    rm -rf "$RUNDIR/input" "$RUNDIR/resamp/*"
    #fi
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

echo "$AVG $USE_BB $TOTAL_FILES $BB_FILES $avg_stagein $sd_stagein $avg_rsmpl $sd_rsmpl $avg_coadd $sd_coadd $avg_total $sd_total" >> $SUMMARY_CSV
=======
if (( $BB > 0 )); then
    cp -r "$RUNDIR/output" "$PWD"
fi

echo "$SEP"

if (( $BB == 1 )); then
    cp -r $RUNDIR/output $PWD
fi

rm -rf *.{fits,xml} resamp/

