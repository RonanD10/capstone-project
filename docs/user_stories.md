## Epic 1 
```text
As a DATA ENGINEER
I want a working end-to-end data pipeline and Streamlit app
So that I have a MVP to add features to 
```

### USER STORY 1 - Extract Data 
```text
As a DATA ENGINEER/ANALYST
I want to be able to access the data from the .csv file
So that it can be transformed ready for analysis
```

- [x] Data is extracted from the .csv file  
- [x] Data is extracted in less than 30s 
- [x] Extraction occurs without errors and data integrity is maintained
- [x] Successful extractions are logged
- [x] Data is stored in Pandas DataFrame for further processing
- [x] Unit tests are written to verify the data extraction process 
- [x] Component tests are written to verify the data extraction process 


### USER STORY 2 - Data Cleaning 
```text
As a DATA ENGINEER/ANALYST
I want to have clean data, standardised data 
So that it can be analysed easier
```
- [x] Thorough data cleaning exploration has been conducted 
- [x] All missing values have been handled
- [x] All duplicates have been handled
- [x] All strings have been standardised
- [x] All categorical variables have been handled 
- [x] Cleaning occurs without errors and data integrity is maintained
- [x] Unit tests are written to verify the data cleaning process 


### USER STORY 3 - Load Data 
```text
As a DATA ENGINEER/ANALYST
I want be able to load the transformed data to the Streamlit
So that it can be used for analysis and visualisation
```
- [x] Data is loaded from the transformed .csv file  
- [x] Data is loaded in less than 30s 
- [x] Loading occurs without errors and data integrity is maintained
- [x] Successful loadings are logged
- [x] Data is stored in Pandas DataFrame for further processing
- [x] Unit tests are written to verify the data loading process 

### USER STORY 4 - Streamlit App 
```text
As a DATA ENGINEER/ANALYST
I want be able to run a Streamlit app 
So that the data can be visualised and analysed 
```        
- [x] run_app script launches app and uses loaded data 
- [x] Basic homepage is displayed with a placeholder title
- [x] Second page 
- [x] Basic side bar for navigating between pages

### USER STORY 5 - Basic Visualisations
```text
As a DATA ENGINEER/ANALYST
I want be able to display data visualisatons
So that data insights can be easily understood 
```
- [x] Display ordered bar chart of avg age per event (summer and winter separate)
- [x] Display line chart of avg gold medalist age per Olympics


## Epic 2
```text
As a SPORTS DATA ANALYST
I want insightful visualisations and a multi-feature Streamlit app  
So that I can obtain detailed insights and offer a quality user experience
```
  
### USER STORY 1 - App Feature 1
```text
As a SPORTS DATA ANALYST
I want the web app to have an optimal athlete builder for any sport
So that I can make better team selections
```
- [x] Display an athlete attribute profile (age, height, weight)
- [x] Create attribute input selection options (gender, sport, event)
- [x] Display selected attributes on profile 
- [x] Display two profiles vertically with title of {sport} - {event}

### USER STORY 1 - App Feature 2 
```text
As a SPORTS DATA ANALYST
I want to be able to analyse medal data over time and by country
So that I can better understand the history of the games
```
- [x] Add noc -> country transformation to pipeline 
- [x] Summer and Winter bar chart of top 10 countries by total medals 
- [x] Summer and Winter tables bar chart of top 10 country by total gold medals
- [x] Top 10 athletes with most medals
- [x] Geographic plot of some kind
  
  ### USER STORY 1 - App Feature 3
```text
As a SPORTS DATA ANALYST
I want to have fun facts 
So that my app offers a variety of analyses
```
- [x] Search dataset to back up facts
- [x] List facts on fun facts page
  

<!-- ### USER STORY 1 - App Feature 4
```text
As a SPORTS DATA ANALYST
I want Olympic-themed styling 
So that the app is visually pleasing and consistent
``` -->

<!-- ### USER STORY 1 - App Feature 5 
```text
As a SPORTS DATA ANALYST
I want to be able analyse the record progression across each athletics event
So that I can better understand the progress made in each sport 
```
- [ ] Create line graph of Olympic record progression for 100m 
- [ ] Create line graphs of Olympic record progression for 200m, 400m, 800m
- [ ]  -->


## Epic 3
```text
As a DATA ENGINEER
I want full happy-path unit test coverage and clean linting
So that the project code is robust and conforms to PEP8 standards 
```
- [x] Unit testing for every function
- [x] Linting passes for all files 


## Future Improvements
- Further testing: component, integration, e2e, and more unit test breadth 
- Further insights on app, including analysis of performance details - which requires an additional dataset and transformation features
- More scalable app page logic to make multi-layered pages with robust logic for interactive features

