/*
* Copyright 2014 Mortar Data Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "as is" Basis,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/*
 This forces a reduce task that will divide our output up into $redshift_parallelization
 number of files.  This allows us to take advantage of Redshift's ability to
 load multiple files at once.

 input_data: data to be split into partitions
 grouping_key: key to group the data upon
 redshift_parallelization: number of partitions to create
*/
define split_for_redshift(input_data, grouping_key, redshift_parallelization) returns reduced {
    grouped = group $input_data
                 by ($grouping_key)
           parallel $redshift_parallelization;
    $reduced = foreach grouped
             generate flatten($input_data);
};
