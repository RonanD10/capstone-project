# Project Kanban 

```mermaid
kanban
    To do       
        Deploy Streamlit app 

    In progress
        Create README with setup and usage instructions + instructions for accessing the data source
        Check over everything 
        Add doc strings to main functions    
        
    Done Epic 1
        Decide which dataset
        Create GitHub repo 
        Set up project skeleton
        Set up older Python 3.12 virtual environment for Streamlit and install requirements
        Extract data successfully
        Write test to verify data extraction process
        Conduct thorough data cleaning exploration
        Configure run_etl in .toml
        Create transform function to implement data cleaning exploration steps
        Write unit tests to verify data cleaning process 
        Load data successfully
        Run Streamlit app successfully
        Create basic app homepage with placeholder title
        Create second app page and sidebar for page navigation
        Automate etl -> app script
        Display ordered bar chart of avg age per event (summer and winter separate)
        Display line chart of avg gold medalist age per Olympics

    Done Epic 2 
        Display an athlete attribute profile - age, height, weight
        Create attribute input selection options - gender, sport, event
        Display selected attributes on profile 
        Ensure testing runs properly
        Add noc -> country transformation to pipeline 
        Summer and Winter bar chart of top 10 countries by total medals
        Summer and Winter tables bar chart of top 10 country by total gold medals
        Summer and Winter bar chart of most games won
        Top 10 athletes with most medals
        Search dataset to back up facts
        List facts on fun facts page
        Fix event string formatting
    
    Done Epic 3
        Write extract unit tests
        Write transform unit tests
        Write remaining unit tests
        Tidy up notebooks 
        Fix PEP8 errors

```

# Learning Backlog  
```mermaid
kanban
    To do 
        Better understand data aggregation/enrichment techniques/practises
        Better awareness of exploratory data analysis techniques/ideas
        Understand .coveragerc, .flake8, .gitignore
        Read logger documentation
        Better understand testing - comb examples  
        
    In progress  
        
        

    Done
        Comb all resources, start to finish
        Better understand Kanban planning and its relationship to epics, user stories
        Better understand unittest.mock, pytest fixture 
        Better understand data cleaning techniques/practised
        Understand all functions in run_etl
```

### Presentation 
- Focus on the project approach and the project development
- See rubric: The following are graded on a 0-3 basis (0 = not at all, 1 = partially, 2 = explained, 3 = explained in depth)
    - Goals Outlined
    - Choice of Data
    - Extract
    - Transform
    - Load
    - Technical Depth (including testing)
    - Steamlit
    - Insights
    - Challenges and Takeaways
    - Future Dev
    - Flow
    - Presentation Style