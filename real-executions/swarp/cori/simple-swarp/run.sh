#!/bin/bash
#SBATCH --qos=debug
#SBATCH --constraint=haswell
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0:30:00
#SBATCH --job-name=swarp-simple
#SBATCH --output=output.%j
#SBATCH --error=error.%j
#SBATCH --mail-user=lpottier@isi.edu
#SBATCH --mail-type=FAIL
#SBATCH --switches=1
#SBATCH --exclusive=user
#SBATCH --dependency=singleton
#SBATCH --hint=nomultithread

function usage() {
    echo "usage: $0 [[[-r=num of runs] [-s=num of input files to stage in (0 <= s <= 32)]  [-b=private | striped]] | [-h]]"
}

function checkbb_striped() {
    if [ -z "$DW_JOB_STRIPED" ]; then
        echo "Error: no striped burst buffer allocation found. Use Slurm  --bbf=file option."
        exit
    fi
}

function checkbb_private() {
    if [ -z "$DW_JOB_PRIVATE" ]; then
        echo "Error: no private burst buffer allocation found. Use Slurm  --bbf=file option."
        exit
    fi
}

## $1 is the input directory : ex. input/
## $2 is the file where to write the files to stage in .fits + weight.fits
## $3 is the number of files to stage in from 0 to 32 (only even number allowed, 0, 2, 4, ..., 30, 32)
## $4 is the file where we write the current location to input file (to give as input to swarp resample)
function get_list_file() {
    rm -rf  "$2"
    rm -rf  "$4"
    count=1
    touch "$2"
    touch "$4"
    for i in $(ls "$1"); do 
        if (( $count > "$3" )); then
            if ( ! echo "$i" | grep -q ".w.weight." ); then
                echo -n "$(pwd)/$1/$i " >> "$4"
            fi
        else
            echo "$(pwd)/$1/$i" >> "$2"
            if ( ! echo "$i" | grep -q ".w.weight." ); then
                echo -n "@INPUT@/$i " >> "$4"
            fi
            count=$(echo "$count + 1" | bc -l)
        fi
    done
    echo ""
}

## $1 is the file that contains the name of each file to stage in
## $2 contains the destination directory
function stage_in_files() {
    if [ ! -f "$1" ]; then
        echo "[error] file $1 does not exist"
        exit
    fi
    
    if [ ! -f "$1" ]; then
        echo "[info] directory $2 does not exist. mkdir.."
        mkdir -p "$2"
    fi
    size_total=0
    local size=0
    for i in $(cat "$1"); do
        size=$(du -h "$i" | sed "s/\(^[0-9]*\.*[0-9]*\).*/\1/")
        cp -f -p "$i" "$2"
        echo "OK: $i -> $2 [$(du -h "$i" | sed "s/\(^[0-9]*\.*[0-9]*[A-Z]\).*/\1/")]"
        size_total=$(echo "$size_total + $size" | bc -l)
    done
}

BB=0
BB_FILES=0

for i in "$@"; do
    case $i in
            -r=*|--run=*)
                AVG="${i#*=}"
                shift # past argument=value
            ;;
            -s=*|--stage=*)
                NBFILES="${i#*=}"
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
                BB_FILES=32
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

if [ -z "$NBFILES" ]; then
    NBFILES=0
fi

if (( $NBFILES > 32 )); then
    NBFILES=32
elif (( $NBFILES < 0 )); then
    NBFILES=0
fi

PWD=$(pwd)
TOTAL_FILES=64 #64 files per pipeline
SRUN="srun -N 1 -n 1"

OUTDIR="$RUNDIR/output_$SLURM_JOB_ID"
OUTDIR_PWD="$PWD/output_$SLURM_JOB_ID"

if (( $BB == 1 )); then
    CSV="$OUTDIR/data-bb-striped.csv"
    SUMMARY_CSV="$OUTDIR/summary-bb-striped.csv"
elif (( $BB == 2 )); then
    CSV="$OUTDIR/data-bb-private.csv"
    SUMMARY_CSV="$OUTDIR/summary-bb-private.csv"
fi

CSV_PFS="$OUTDIR_PWD/data-pfs.csv"
SUMMARY_CSV_PFS="$OUTDIR_PWD/summary-pfs.csv"

mkdir -p $RUNDIR/resamp
mkdir -p $OUTDIR
mkdir -p $OUTDIR_PWD
mkdir -p $PWD/resamp

STAGE_LIST="$OUTDIR_PWD/stage.txt"
INPUT_LIST="$OUTDIR_PWD/input.txt"
BB_FILES=$(echo "32 + $NBFILES" | bc -l)

get_list_file "input/" "$STAGE_LIST" "$NBFILES" "$INPUT_LIST";
sed -i -e "s|@INPUT@|$RUNDIR/input/|g" $INPUT_LIST
#cat "$INPUT_LIST"
echo ""
#stage_in_files "$STAGE_LIST" "$OUTDIR";
#echo $size_total

input_rsmpl="$(cat $INPUT_LIST)"
output_rsmpl="$OUTDIR/output_resample.log"
input_coadd="$RUNDIR/resamp/PTF201111*.w.resamp.fits"
output_coadd="$OUTDIR/output_coadd.log"

input_rsmpl_pfs="$PWD/input/PTF201111*.w.fits"
output_rsmpl_pfs="$OUTDIR_PWD/output_resample_pfs.log"
input_coadd_pfs="$PWD/resamp/PTF201111*.w.resamp.fits"
output_coadd_pfs="$OUTDIR_PWD/output_coadd_pfs.log"

if (( $BB == 1 )); then
    config_rsmpl="$RUNDIR/config/resample-bb.swarp"
    config_coadd="$RUNDIR/config/combine-bb.swarp"
elif (( $BB == 2 )); then
    config_rsmpl="$RUNDIR/config/resample-bb-priv.swarp"
    config_coadd="$RUNDIR/config/combine-bb-priv.swarp"
fi

config_rsmpl_pfs="$PWD/config/resample.swarp"
config_coadd_pfs="$PWD/config/combine.swarp"

rm -rf $RUNDIR/*.fits $RUNDIR/*.xml

SEP="=============================================================================================================="
echo "$SEP"

echo "ID           -> $SLURM_JOB_ID"
echo "RUNDIR       -> $RUNDIR"
echo "OUTDIR       -> $OUTDIR"
echo "CSV          -> $CSV"
echo "SUMMARY_CSV  -> $SUMMARY_CSV"
echo "AVG          -> $AVG"
echo "STAGED_FILES -> $NBFILES"
echo "BB_FILES     -> $BB_FILES"
echo "TOTAL_FILES  -> $TOTAL_FILES"
echo "SRUN         -> $SRUN"
echo "STARTED      -> $(date --rfc-3339=ns)"

echo "$SEP"

echo "ID,$SLURM_JOB_ID" > $OUTDIR/info.csv
echo "RUNDIR,$RUNDIR" >> $OUTDIR/info.csv
echo "OUTDIR,$OUTDIR" >> $OUTDIR/info.csv
echo "CSV,$CSV" >> $OUTDIR/info.csv
echo "SUMMARY_CSV,$SUMMARY_CSV" >> $OUTDIR/info.csv
echo "CSV_PFS,$CSV_PFS" >> $OUTDIR/info.csv
echo "SUMMARY_CSV_PFS,$SUMMARY_CSV_PFS" >> $OUTDIR/info.csv
echo "AVG,$AVG" >> $OUTDIR/info.csv
echo "STAGED_FILES,$NBFILES" >> $OUTDIR/info.csv
echo "BB_FILES,$BB_FILES" >> $OUTDIR/info.csv
echo "TOTAL_FILES,$TOTAL_FILES" >> $OUTDIR/info.csv
echo "SRUN,$SRUN" >> $OUTDIR/info.csv
echo "STARTED,$(date --rfc-3339=ns)" >> $OUTDIR/info.csv

echo "ID RUN USEBB FILES FILESBB SIZEBB STAGEIN RSMPL COADD MAKESPAN" > $CSV
echo "ID RUN USEBB FILES FILESBB SIZEBB STAGEIN STAGE_SD RSMPL RSNPL_SD COADD COADD_SD MAKESPAN MAKESPAN_SD" > $SUMMARY_CSV

echo "ID RUN USEBB FILES FILESBB SIZEBB STAGEIN RSMPL COADD MAKESPAN" > $CSV_PFS
echo "ID RUN USEBB FILES FILESBB SIZEBB STAGEIN STAGE_SD RSMPL RSNPL_SD COADD COADD_SD MAKESPAN MAKESPAN_SD" > $SUMMARY_CSV_PFS


echo -e "ID \t\tRUN \tSTAGEIN (S) \t\tRSMPL (S) \t\tCOADD (S) \t\tTOTAL (S)"

all_stagein=()
all_rsmpl=()
all_coadd=()
all_total=()

all_rsmpl_pfs=()
all_coadd_pfs=()
all_total_pfs=()


#lfs setstripe -c 1 -o 1 $DIR

for k in $(seq 1 1 $AVG); do
    ### BB run

    USE_BB='N'
    time_stagein=0
    size_total=0
    if (( $VERBOSE >= 2 )); then
        echo "[$k] START stagein:$(date --rfc-3339=ns)"
    fi
    t1=$(date +%s.%N)
    cp swarp $RUNDIR
    stage_in_files "$STAGE_LIST" "$RUNDIR/input";
    #echo $size_total
    #cp -r input/ $RUNDIR/
    cp -r config/ $RUNDIR/
    t2=$(date +%s.%N)

    if (( $VERBOSE >= 2 )); then
        echo "[$k] END stagein:$(date --rfc-3339=ns)"
    fi
    BB_FILES=$(echo "32 + $NBFILES" | bc -l)
    USE_BB='Y'
    time_stagein=$(echo "$t2 - $t1" | bc -l)

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

    rm -rf $RUNDIR/resamp/*
    echo "$SLURM_JOB_ID $k $USE_BB $TOTAL_FILES $BB_FILES $size_total $time_stagein $time_rsmpl $time_coadd $time_total" >> $CSV

    #### PFS run

    USE_BB='N'
    if (( $VERBOSE >= 2 )); then
        echo "[$k] No stage in, using PFS."
    fi

    if (( $VERBOSE >= 2 )); then
        echo "[$k] START rsmpl_pfs:$(date --rfc-3339=ns)"
    fi
    
    $SRUN $RUNDIR/swarp -c $config_rsmpl_pfs $input_rsmpl_pfs > $output_rsmpl_pfs 2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END rsmpl_pfs:$(date --rfc-3339=ns)"
    fi

    if (( $VERBOSE >= 2 )); then 
        echo "[$k] START coadd_pfs:$(date --rfc-3339=ns)"
    fi
    
    $SRUN $RUNDIR/swarp -c $config_coadd_pfs $input_coadd_pfs > $output_coadd_pfs  2>&1
    
    if (( $VERBOSE >= 2 )); then
        echo "[$k] END coadd_pfs:$(date --rfc-3339=ns)"
    fi

    time_rsmpl_pfs=$(cat $output_rsmpl_pfs | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_coadd_pfs=$(cat $output_coadd_pfs | sed -n -e 's/^> All done (in \([0-9]*\.[0-9]*\) s)/\1/p')
    time_total_pfs=$(echo "$time_rsmpl_pfs + $time_coadd_pfs" | bc -l)
    
    all_rsmpl_pfs+=( $time_rsmpl_pfs )
    all_coadd_pfs+=( $time_coadd_pfs )
    all_total_pfs+=( $time_total_pfs )

    rm -rf $PWD/resamp/*
    echo "$SLURM_JOB_ID $k $USE_BB $TOTAL_FILES $BB_FILES 0 0 $time_rsmpl_pfs $time_coadd_pfs $time_total_pfs" >> $CSV_PFS

    echo -e "BB  $SLURM_JOB_ID \t$k \t$time_stagein \t\t$time_rsmpl \t\t\t$time_coadd \t\t\t$time_total "
    echo -e "PFS $SLURM_JOB_ID \t$k \t0 \t\t\t$time_rsmpl_pfs \t\t\t$time_coadd_pfs \t\t\t$time_total_pfs "
    
    #if (( $BB == 1 )); then
    #    rm -rf "$RUNDIR/input" "$RUNDIR/resamp/*"
    #fi
done
echo "ENDED,$(date --rfc-3339=ns)" >> $OUTDIR/info.csv

### BB computation 
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

printf "BB:  \t\t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \n" $avg_stagein $sd_stagein $avg_rsmpl $sd_rsmpl $avg_coadd $sd_coadd $avg_total $sd_total

echo "$SLURM_JOB_ID $AVG $USE_BB $TOTAL_FILES $BB_FILES $avg_stagein $sd_stagein $avg_rsmpl $sd_rsmpl $avg_coadd $sd_coadd $avg_total $sd_total" >> $SUMMARY_CSV

### PFS computation 

sum_rsmpl_pfs=0
for i in ${all_rsmpl_pfs[@]}; do
    sum_rsmpl_pfs=$(echo "$sum_rsmpl_pfs + $i" | bc -l)
done

sum_coadd_pfs=0
for i in ${all_coadd_pfs[@]}; do
    sum_coadd_pfs=$(echo "$sum_coadd_pfs + $i" | bc -l)
done

sum_total_pfs=0
for i in ${all_total_pfs[@]}; do
    sum_total_pfs=$(echo "$sum_total_pfs + $i" | bc -l)
done

avg_rsmpl_pfs=$(echo "$sum_rsmpl_pfs / $AVG" | bc -l)
avg_coadd_pfs=$(echo "$sum_coadd_pfs / $AVG" | bc -l)
avg_total_pfs=$(echo "$sum_total_pfs / $AVG" | bc -l)

sd_rsmpl_pfs=0
sd_coadd_pfs=0
sd_total_pfs=0


for i in ${all_rsmpl_pfs[@]}; do
    sd_rsmpl_pfs=$(echo "$sd_rsmpl_pfs + ($i - $avg_rsmpl_pfs)^2" | bc -l)
done

for i in ${all_coadd_pfs[@]}; do
    sd_coadd_pfs=$(echo "$sd_coadd_pfs + ($i - $avg_coadd_pfs)^2" | bc -l)
done

for i in ${all_total_pfs[@]}; do
    sd_total_pfs=$(echo "$sd_total_pfs + ($i - $avg_total_pfs)^2" | bc -l)
done

sd_rsmpl_pfs=$(echo "sqrt($sd_rsmpl_pfs / $AVG)" | bc -l)
sd_coadd_pfs=$(echo "sqrt($sd_coadd_pfs / $AVG)" | bc -l)
sd_total_pfs=$(echo "sqrt($sd_total_pfs / $AVG)" | bc -l)

echo ""

printf "PFS: \t\t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \t%-0.2f (+/- %0.2f) \n" 0 0 $avg_rsmpl_pfs $sd_rsmpl_pfs $avg_coadd_pfs $sd_coadd_pfs $avg_total_pfs $sd_total_pfs

echo "$SLURM_JOB_ID $AVG $USE_BB $TOTAL_FILES $BB_FILES 0 0 $avg_rsmpl_pfs $sd_rsmpl_pfs $avg_coadd_pfs $sd_coadd_pfs $avg_total_pfs $sd_total_pfs" >> $SUMMARY_CSV_PFS

if (( $BB > 0 )); then
    cp -r $OUTDIR/* "$OUTDIR_PWD/"
fi

echo "$SEP"

rm -rf *.{fits,xml} resamp/


