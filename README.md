# Redshift for R

Provides a few functions to make it easier to access our Redshift cluster from R.

This library essentially does very little, other than provide some nicer functions that use the [RJDBC](http://www.rforge.net/RJDBC/index.html) package.

## Installing

[Download the latest code tarball](https://github.com/pingles/redshift-r/archive/master.zip). You can then install the package as follows:

    install.packages("/Users/me/Downloads/redshift-r-master.zip", dependencies=T, repos=NULL, type="source")

## Usage
    require(redshift)
    
    conn <- redshift.connect("jdbc:postgresql://blah.blah.eu-west-1.redshift.amazonaws.com:5439/data", "username", "password")
    # conn is just a regular RJDBC connection object

    # we can retrieve a list of tables
    tables <- redshift.tables(conn)

    # and get some info about the columns in one of those tables
    cols <- redshift.columns(conn, "weblog")

    # we can use RJDBC dbGetQuery to retrieve query results, perhaps from our weblog data
    statuses_by_day <- dbGetQuery(conn, paste("SELECT DATE(timestamp) as dated, status, COUNT(1) as request_count",
                                              "FROM weblog",
                                              "GROUP BY DATE(time_stamp), status",
                                              "ORDER BY dated"))
    statuses_by_day$dated <- as.Date(statuses_by_day$dated)
    statuses_by_day$status <- as.factor(statuses_by_day$status)

    # finally, lets do a little scatter plot to see how this looks
    require(ggplot2)
    p <- ggplot(statuses_by_day, aes(x=dated, y=request_count))
    p + geom_point(aes(color=status))

## Checking package

Before committing changes to the project you can run the following command from the directory containing the cloned repo (i.e. `./redshift/..`)

    $ R CMD check redshift

## To Do

* Update R documentation with functions- generates warnings during check at the moment.

## License

This R package includes the Postgresql JDBC binary which is distributed under the following license:

    Copyright (c) 1997-2011, PostgreSQL Global Development Group
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice,
       this list of conditions and the following disclaimer.
    2. Redistributions in binary form must reproduce the above copyright notice,
       this list of conditions and the following disclaimer in the documentation
       and/or other materials provided with the distribution.
    3. Neither the name of the PostgreSQL Global Development Group nor the names
       of its contributors may be used to endorse or promote products derived
       from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
    LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
    SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
    ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.

This Redshift R code is distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html).