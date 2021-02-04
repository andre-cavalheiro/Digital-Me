# Intro
This project, developed as a part of my MSc thesis, aims to process and organize personal data generated from multiple web platforms. We propose two resulting data structures:
        - A MongoDB database
        - A Knowledge Graph (with NetworkX and Neo4j support)

Currently, the following platforms are supported:
- Google Search
- YouTube
- Facebook
- Twitter
- Reddit

To use it on your data, you need to download your own personal files from each platform (You need not have files from all the listed platforms to use this project).
This document will only serve as a quick description of what it was made, for a more in depth review of the project and thinking behind it - please read the actual paper [1].


# Processing into MongoDB
## Content Collection
Most personal data files have a structure focused on user actions and content.Some standardization is required in order to accommodate data from different platforms. 

We convert every file into a list of objects. Each object represents a piece of content that's present in the user's history. The following attributes are mandatory.
```python
{
    'platform': str,
    'timestamp': [],
    'type': str,
    'body': str,
    'tags': [],
    'sources': [],
    (...)
}
```



Others may be added (depending on both content type and platform) on the condition that they improve the description of the content they're attached to.
The __timestamp__ attribute is a list of timestamps, one for appearance of said content in the user's history.
Examples of content objects
The content list (which in practice is just a list of dictionaries) can be easily saved to MongoDB.

## Source Collection
Online content can usually be traced back to a source - the entity responsible for either creating or distributing said content. Some of these files hold that information. We also create a MongoDB collection focused on content sources. Each one with the following format:
```python
{
    'label':
    'type':
    'platform':
    'associatedContent':
    'associatedContentTypes':
}
```
Examples of source objects
## Tag Collection
Finally, to provide conceptual context to content objects, we offer the possibility of performing entity extraction on content payloads to create a tag collection. Each tag, which is just an extracted entity, can be associated with different contents. For this, we use the NLP API [Rosette Analytics](https://developer.rosette.com/). Each tag ends up with the following format:
```python
{
	'associatedContent': []
	'mentionForms': []
	'normalizedForms': []
	'types':
	'QID':
} 
```
Examples of tag objects

# Graph conversion
Previously we defined three types of objects: content, sources, and tags.  In order to accommodate the temporal aspect we introduce a 4th type of object: time, each one representing a day  in the user's life. 

We can now transform the user's history into a network using the validating schema that describes the interactions between these object types. Each object type is represented by a shape, and the interactions between them as connections between those shapes.


<figure>
  <img src="docs/images/mainShape.png" alt=""/>
  <figcaption>Fig 1. </figcaption>
</figure>  


Shapes graph in UML-like format. Each box (rectangle) represents a shape that is bounded by a set of constraints. Nodes conform to a shape if and only if they satisfy all constraints. 
There are two types of constraints: the existence of certain attributes within a shape or a connection and the number of nodes conforming to a particular shape that the conforming node can relate to via a given edge. 
For example:
    - [0..1] denotes either no connection or precisely one
    - [1..*] denotes one-to-many
    - [1..1] denotes precisely one

In order to further to improve the descriptive power of the network, we define a hierarchy of terms for each object class. These will serve as the terminology  of the network, providing semantic value to the connections in it. The __type__ attributes in each node or edge hold the values of leaf in the respective hierarchy presented in Figs X or Y.

<figure>
  <img src="docs/images/nodeHierarchy.png" alt=""/>
  <figcaption>Fig 2. </figcaption>
</figure>  

<figure>
  <img src="docs/images/edgeHierarchy.png" alt=""/>
  <figcaption>Fig 3. </figcaption>
</figure>  

The resulting network is a heterogeneous graph, guaranteed to have a single component due to the chain of daily nodes, where tag and source nodes are connected to days via content nodes. 

If it weren't for the connections between time nodes, the network would be a tripartite graph, given that, that's the only edge type that connects nodes of the same class. 
