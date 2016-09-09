#!/usr/bin/env python

"""Accessor for runtime configuration settings.

In general one should be able to simply call get_configuration()

"""
# This file is part of PASTA and is forked from SATe

# PASTA, like SATe, is free software: you can redistribute it and/or modify
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

sc = None


def init_spark():
    """
    Init the SPARK context if we are running in a spark cluster
    :return: The SparkContext, if we are using Spark; false otherwise
    """
    try:
        # noinspection PyUnresolvedReferences,PyUnresolvedReferences
        from pyspark import SparkConf, SparkContext
    except ImportError:
        return False
    global sc
    if not sc:
        conf = SparkConf()
        conf.set("spark.app.name", "PASTA Spark")
        sc = SparkContext(conf=conf)
    return sc


def finish_spark():
    """
    Set the SparkContext to None so avoid to use again
    """
    global sc
    sc = None


def get_sparkcontext():
    """
    Get the actual SparkContext
    :return: the SparkContext
    """
    global sc
    return sc


def spark_align(joblist):
    import os
    global sc
    from scheduler import LightJobForProcess

    lightjoblist = list()
    for job in joblist:
        pa = job.start()

        # pa[0][-1] += '/part-00000'
        # data = sc.sequenceFile(pa[0][-1], 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')
        # lightjoblist.append([LightJobForProcess(pa[0], pa[1], os.environ), data.collectAsMap()])
        # Read the input.fasta file and store as a list
        with open(pa[0][-1], 'r') as f:
            data = f.readlines()
        # Create a list of pairs (job, data)
        lightjoblist.append((LightJobForProcess(pa[0], pa[1], os.environ), data))

    # Parallelize the list of pairs (job, data)
    rdd_joblist = sc.parallelize(lightjoblist)

    # For each pair, do alignment
    rdd_joblist.foreach(do_align)

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
    tmpfile = NamedTemporaryFile()
    # write the data
    tmpfile.writelines(job[1])
    # write_to_local(job[1], tmpfile)
    # Change the name of the input file
    job[0]._invocation[-1] = tmpfile.name
    tmpfile.flush()
    # Run the job
    job[0].run()
    tmpfile.close()


def write_to_local(data, dest):
    """Writes the data in FASTA format to a temporary file"""
    for name in data.keys():
        dest.write('>%s\n%s\n' % (name, data[name]))
