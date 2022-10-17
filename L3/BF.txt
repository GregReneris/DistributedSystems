"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Nicholas Jones
:Version: fq19-01
"""

class BellmanFord(object):
	
	def __init__(self, init_graph):
		"""
		:param init_graph: the graph to use
		"""
		self.graph = init_graph
		self.vertices = len(init_graph)
	
	def shortest_paths(self, origin, tolerance=0):
		"""
		"""
		# construct a list of distances
		dist = {}
		prev = {}
		
		# initialize the shortest distance to infinity and previous vertex to None
		for vertex in self.graph:
			dist[vertex] = float("Inf")
			prev[vertex] = None
		
		# the shortest distance to the origin from the origin is 0
		dist[origin] = 0
		
		for i in range(self.vertices - 1):
			for curr1 in self.graph:
				for curr2 in self.graph[curr1]:
					weight = self.graph[curr1][curr2]["price"]
					
					# if this new path is shorter than the previous distance
					if dist[curr1] != float("Inf") and (dist[curr1] + weight + tolerance < dist[curr2] and dist[curr1] + weight - tolerance < dist[curr2]):
						dist[curr2] = dist[curr1] + weight
						prev[curr2] = curr1
						
		# negative path detection
		for curr1 in self.graph:
			for curr2 in self.graph[curr1]:
				weight = self.graph[curr1][curr2]["price"]
				
				if dist[curr1] != float("Inf") and (dist[curr1] + weight + tolerance < dist[curr2] and dist[curr1] + weight - tolerance < dist[curr2]):
					# we found a negative path, return it
					return dist, prev, (curr1, curr2)
					
		return dist, prev, None