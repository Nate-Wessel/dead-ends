# run tarjan with igraph
# this part detects proper dead-ends and writes a CSV with the component identity for each edge
# dead-ending segments have component = -1

# name of the region/city table
city = 'helsinki'

modes = c('car','bike','foot')

# connect to database
library("RPostgreSQL")
driver <- dbDriver("PostgreSQL")
connection <- dbConnect(driver, dbname="", password="", user="", host="")

# load the graph library
library('igraph')

# run for each mode
for(mode in modes){

	print(paste('constructing the',city,mode,'graph...'))

	# data goes here
	output_file = paste0('~/AAG2016/data/out/',city,'_',mode,'_out.csv')

	# get only the edges for the current mode
	if(mode=='car'){
		where_statement = 'WHERE flags % 2 = 1;'
	}else if(mode=='bike'){
		where_statement = "WHERE flags IN (2,3,6,7);"
	}else if(mode=='foot'){
		where_statement = "WHERE flags >= 4;"
	}

	# get the relevant edges
	d <- dbGetQuery(connection,
		paste0(
			"SELECT source, target, km AS weight ",
			"FROM ",city," ",
			where_statement
		)
	)

	# turn into a matrix
	m = as.matrix(d[,1:2])
	# turn into a vector
	vd = c(t(m))

	# make the graph
	g = make_graph(vd,directed=F)
	# add weights to the edges, for later
	#E(g)$weight = d$weight

	# clean up vars to save space in RAM
	d = m = vd = NULL

	# run the big time-consuming function
	print(paste('traversing the',city,mode,'graph...'))
	bc = biconnected_components(g)

	# select only components with more than 100 edges
	mc = bc$component_edges[lengths(bc$component_edges)>100]

	# give each edge a component attribute defaulting to -1
	E(g)$component = -1

	# loop over major connected components
	# assinging real component id's to major connected components
	for(i in 1:length(mc)){
		# as.vector(mc[[i]]) returns the ids/keys of the edges
		E(g)[as.vector(mc[[i]])]$component = i
	}

	# convert the graph to a dataframe of edges and attributes
	results_table = get.data.frame(g)
	# ignore the weights field
	results_table = results_table[,c('from','to','component')]
	# output the results
	write.csv(results_table,output_file)

} # for each mode

dbDisconnect(connection)
