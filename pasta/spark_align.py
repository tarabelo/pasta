#!/usr/bin/env python

"""Alignements in SPARK
"""

# This file is part of PASTASPARK and is forked from PASTa

# PASTASPARK like PASTA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Tomas F. Pena and Jose M. Abuin, University of Santiago de Compostela (Spain)
import os

from configure_spark import get_sparkcontext
from scheduler import LightJobForProcess


def spark_align(joblist):
    sc = get_sparkcontext()

    lightjoblist = list()
    for job in joblist:
        pa = job.start()
        # Read the input data
        # TODO Avoid to use the disk and create the list lightjoblist directly
        with open(pa[0][-1], 'r') as f:
            data = f.readlines()
        # Create a list of pairs (job, data)
        lightjoblist.append((LightJobForProcess(pa[0], pa[1], os.environ), data))

    # Parallelize the list of pairs (job, data)
    rdd_joblist = sc.parallelize(lightjoblist)
    # for j in lightjoblist:
    #    do_align(j)

    # For each pair, do alignment
    # as output, get an RDD with pairs (out_filename, out_data)
    out = rdd_joblist.map(do_align)

    # Collect out and write the data to corresponding files in master's disk
    results = out.collect()
    for res in results:
        with open(res[0], mode='w+b') as f:
            f.writelines(res[1])

    # Finish the jobs
    for job in joblist:
        job.results = job.result_processor()
        job.finished_event.set()
        job.get_results()
        job.postprocess()


def do_align(job):
    global sc
    # Save data in local disc in a temporary file
    from tempfile import NamedTemporaryFile
    intmp = NamedTemporaryFile()
    # outtmp = NamedTemporaryFile()
    # write the  inputdata
    intmp.writelines(job[1])
    # write_to_local(job[1], tmpfile)
    # Change the name of the input file
    job[0]._invocation[-1] = intmp.name
    intmp.flush()
    # Change the name of the output file
    outfile = job[0]._k['stdout']
    # Run the job and Save in the rdd the name and data of the output file
    out = (outfile, job[0].runwithpipes())
    intmp.close()
    #outtmp.close()
    return out


def write_to_local(data, dest):
    """Writes the data in FASTA format to a temporary file"""
    for name in data.keys():
        dest.write('>%s\n%s\n' % (name, data[name]))
