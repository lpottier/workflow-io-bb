#!/bin/bash
tmp=TMPFILE
dirs="swarp-run-1N.hAcVVS
swarp-run-1N.snDrMC
swarp-run-2N.UpmDQH
swarp-run-2N.coGgNx
swarp-run-4N.ITzA2j
swarp-run-4N.LfbxSZ
swarp-run-8N.IIy6K6
swarp-run-16N.ZzFEvD
swarp-run-32N.nM0mZN
swarp-run-32N.dB51nA
swarp-run-64N.gwlTor
swarp-run-64N.mLh7Va
"

#swarp-run-64N.mLh7Va

for str in "resample" "combine"; do
    echo "$str"
    for dir in $(find . -name "swarp-run-*"); do
    #for dir in $dirs; do
	coadd=$(find ${dir} -name '*ipm*' | xargs grep -l ${str})
	grep ipm_noregion -h -A 14 ${coadd} > ${tmp}
	iotime=$(grep POSIXIO ${tmp} | cut -c 30-40 | awk '{sum+=$1} END {print sum/NR}')
	wtime=$(grep wtime ${tmp} | cut -c 48-58 | awk '{sum+=$1} END {print sum/NR}')
	#iotime=$(grep POSIXIO ${tmp} | cut -c 30-40 | awk 'BEGIN {max=0} {if ($1>max) max=$1} END {printf "%f\n", max}')
	#wtime=$(grep wtime ${tmp} | cut -c 48-58 | awk 'BEGIN {max=0} {if ($1>max) max=$1} END {printf "%f\n", max}')
	computetime=$(echo $wtime-$iotime | bc -l)
	timeinio=$(echo "(100*$iotime)/$wtime" | bc -l)
	echo "dir=$dir, wall time=$wtime, io time=$iotime, compute time=$computetime, %io=$timeinio"
	#for fn in "fopen" "fclose" "fread" "fwrite" "fseek"; do
	#    grep $fn $tmp | awk -v fn=$fn '{sum+=$6} END {printf "%s: %.2f ", fn, sum/NR}'
	#done
	#echo ""
    done
done
