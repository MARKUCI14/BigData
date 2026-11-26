# The effect of environmental temperature on the Clouded Apollo population size in Romania

This file records the preliminary proposal. For the current repository architecture, data contracts, and differences between the proposal and implemented code, see [Architecture.md](Architecture.md) and the [documentation index](docs/README.md).

The worldwide decline of the native insect population size started to become an ecological
problem more than half a century ago. Some insect groups are more sensitive than others, such as
the hoverfly (Syrphidae) and bumblebee (Bombus) families or the Lepidoptera order, which includes
butterflies and months. This can potentially lead to a new species composition, one which is
disadvantageous to the native groups and advantageous for the more heat-tolerant alien species.
Romania is not an exception from this (Haris et al., 2024; Vizauer et al., 2025).
For the project a representative of the Lepidoptera order was chosen, the butterfly named
clouded Apollo (Parnassius mnemosyne). This is one of the protected species from the Romanian
Red List of Lepidoptera (Rákosy et al., 2021). We hypothesized that, butterflies being a sensitive
group, there will be a strong correlation between the temperature changes and the population size.
For data acquisition two sources were used: INaturalist and Climate Data Store – Copernicus.
● INaturalist is a free platform used by scientists and hobbyists alike. Users can upload images
about plants and animals and have these identified by experts [1]. We downloaded the list
of clouded Apollo observations in Romania from 2000 to 2026, broken down to months and
counties.
● Climate Data Store – Copernicus is a site that contains a wide range of publicly available
climate data. The datasets are quality-assured, they are provided from a variety of sources
and reanalyzed by ECMWF (European Centre for Medium-Range Weather Forecasts). These
originate from satellite data, in-situ observations, commercial plane and ship recordings, etc.
[2]. We downloaded the list of recorded temperatures in Romania from 2000 to 2026,
averaged for months and counties.

Problem Statement
Environmental temperature is an important factor that can influence the distribution, activity, and
survival of insect species. Understanding the relationship between temperature variation and
butterfly occurrence can contribute to ecological monitoring and the identification of potential
environmental pressures on protected species.
However, investigating this relationship requires combining information collected by different
systems. Butterfly observations are recorded through citizen-science platforms, while temperature
measurements are provided by climate data services. These datasets differ in their original formats,
collection methods, spatial coverage, and temporal characteristics.
The main problem addressed by this project is how to integrate and process butterfly observation
records and environmental temperature data to investigate their relationship across Romanian
counties and over time.

The project will focus on the following research question:
To what extent is environmental temperature associated with changes in the number of reported
Clouded Apollo (Parnassius mnemosyne) observations in Romania?
The main hypothesis is that changes in environmental temperature are associated with changes in
the reported occurrence of the Clouded Apollo. Since butterflies can be sensitive to environmental
conditions, a relationship between temperature variation and observation counts may be
identifiable through the analysis of historical data.
The project will examine this relationship using monthly and county-level data covering the period
from 2000 to 2026, subject to the availability and completeness of the collected records.
It is important to distinguish reported observation counts from actual population size. The number
of observations recorded on a citizen-science platform does not directly measure the number of
individual butterflies in the wild. Observation counts may also be influenced by observer activity,
accessibility of locations, seasonal behavior, and differences in data collection effort. Therefore, the
analysis will investigate reported occurrence patterns rather than assume that observation counts
represent exact population sizes.

Motivation and Expected Impact
The motivation for this project is the need for accessible and systematic methods of analyzing
environmental data to support biodiversity monitoring. Protected species can be affected by long-
term environmental changes, and identifying potential relationships between climate variables and
species occurrence can provide useful information for future ecological investigations.
The Clouded Apollo was selected because it is a protected butterfly species included in the
Romanian Red List of Lepidoptera (Rákosy et al., 2021). Investigating its occurrence in relation to
temperature changes provides a specific use case for integrating ecological and climate datasets
within a Big Data architecture.
From a scientific perspective, the project aims to identify temporal and spatial patterns in Clouded
Apollo observations and determine whether these patterns are associated with temperature
variation. The results may help identify counties or periods that deserve further investigation by
researchers and conservation specialists.
From a technical perspective, the project provides an opportunity to demonstrate how
heterogeneous data sources can be integrated into a unified processing pipeline. Historical data will
support batch processing and trend analysis, while a simulated stream of incoming records will
demonstrate how new information can be processed continuously.
The expected outcomes include:
● Integration of butterfly observation data and environmental temperature data from
different sources.
● Cleaning, transformation, and standardization of the collected datasets.
● Analysis of monthly and county-level observation patterns.
● Investigation of the relationship between temperature variation and reported butterfly
occurrence.

● Development of a preliminary Big Data architecture supporting historical and near-real-time
processing.
● Creation of visualizations that communicate the main analytical results.
● Documentation of the data processing workflow and the responsibilities of each team
member.
The project is intended to demonstrate the practical value of Big Data technologies in environmental
monitoring. Although the analysis may reveal associations between temperature and reported
occurrence, it will not automatically establish that temperature changes cause changes in the actual
butterfly population.

Architecture
The proposed architecture will combine batch processing of historical data with near-real-time
processing of incoming weather measurements. This design follows the main objective of the course
project: integrating multiple data sources into a coherent platform that supports data ingestion,
storage, processing, analytics, and visualization.
Architecture Components
1. Data Sources
The system will receive data from three inputs:
● iNaturalist: historical butterfly observation records.
● Copernicus Climate Data Store: historical climate and temperature data.
● Weather API or simulated weather stream: incoming temperature measurements for near-
real-time processing.
2. Data Ingestion Layer
The historical datasets will be imported through batch ingestion scripts. These scripts will retrieve or
load the source files and transfer them into the storage layer.
Apache Kafka is proposed for the streaming ingestion component. It will receive incoming weather
records and make them available to downstream consumers. This will provide a mechanism for
handling continuously arriving events.
3. Batch Storage Layer
A data lake will store the historical datasets, cleaned records, and processed outputs. The data lake
may use a local or cloud-based storage solution, depending on the available infrastructure.
Data will be organized into raw, cleaned, and processed stages. Keeping the original data separate
from transformed datasets will improve traceability and allow processing steps to be repeated.
4. Speed Storage Layer
MongoDB is proposed as a low-latency storage option for processed weather summaries and recent

analytical results. This layer will support quick access by the visualization application without
requiring repeated processing of the entire historical dataset.
5. Batch Processing Layer
Apache Spark is proposed for processing the historical climate and butterfly datasets. It will support
data cleaning, aggregation, integration, and trend analysis.
The batch pipeline will generate monthly and geographical summaries that can be used for statistical
analysis and visualization.
6. Real-Time Processing Layer
Spark Structured Streaming is proposed for processing incoming weather events from Kafka. The
streaming pipeline will validate incoming records, calculate relevant summaries, and write the
results to the speed storage layer.
This component will demonstrate the processing of newly arriving data without waiting for a
complete historical batch to finish.
7. Serving and Visualization Layer
Streamlit is proposed as the visualization interface. It will present the results through charts, tables,
and other visual summaries.
The interface may include historical temperature trends, butterfly observation counts, county-level
comparisons, and recent weather summaries from the streaming pipeline.
The technology choices described above are preliminary and may be refined during the next project
stage according to implementation constraints and the team&#39;s requirements.
Architecture Diagram

Team name: Team work dream work
Team members and roles
The team is composed by 5 people from 2 masters; the tasks being distributed as such:
● Alixandrescu Elena-Adina (Bioinformatics Applied in Lifesciences)
● Faur Ucu-Mihai (Bioinformatics Applied in Lifesciences)
● Gergely Kinga (Bioinformatics Applied in Lifesciences)
● Portik Márk-Krisztián (Software Engineering)
● Zoltáni Szabolcs (Software Engineering)
Students from Bioinformatics Applied in Lifesciences will do the acquisition, sorting and cleaning of
the datasets, while the students from Software Engineering will be planning the architecture and
pipelines.
Bibliography:
1. https://www.inaturalist.org/pages/about
2. https://climate.copernicus.eu/climate-data-store
Haris, A., Józan, Z., Roller, L., Šima, P., &amp; Tóth, S. (2024). Changes in Population Densities and Species
Richness of Pollinators in the Carpathian Basin during the Last 50 Years (Hymenoptera, Diptera,
Lepidoptera). Diversity, 16(6). https://doi.org/10.3390/d16060328
Rákosy, L., Kovács, S., Stǎnescu, M., Székely, L., Corduneanu, C., Crişan, A., &amp; Dincă, V. (2021). Lista
roşie a fluturilor din România = Romanian red list of lepidoptera.
https://www.researchgate.net/publication/373633048_Romanian_Red_List_of_Lepidoptera_2
021
Vizauer, T.-C., Crișan, A., &amp; Rákosy, L. (2025). The status of protected butterfly species in Romania in
the context of national and EU legislation. Entomologica Romanica, 29, 48–65.
https://doi.org/10.24193/entomolrom.29.27
