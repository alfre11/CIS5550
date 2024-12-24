const previousSearchesQueue = [];
const MAX_QUEUE_SIZE = 10;


const resultsPerPage = 5;
let currentPage = 1;

//cache previous results
let prevResults = new Map();

async function fetchRankedData(query) {
    //check previous results
    if(prevResults.has(query)) {
        console.log("previous results found: " + query);
        return prevResults.get(query);
    }

    try {
        const response = await fetch(`http://localhost:8080/ranks?query=${encodeURIComponent(query)}`);
        if (!response.ok) {
            throw new Error('Error fetching ranked data.');
        }
        const data = await response.json();
        console.log("fetched data", data);
        return data;
    } catch (error) {
        console.error('Failed to fetch ranked data:', error);
        return [];
    }
}

async function performSearch() {
    const query = document.getElementById('searchInput').value.trim();
    if (!query) {
        alert('Please enter a search term.');
        return;
    }

    console.log("display loading")

    const loadingSpinner = document.getElementById('loadingSpinner');
    loadingSpinner.style.display = 'block';

    let rankedResults = "";

    try {
        rankedResults = await fetchRankedData(query);
        console.log(rankedResults.length);
        addQueryToQueue(query);
        updateSearchHistory();

        if (rankedResults.length === 0) {
            document.getElementById('resultsContainer').innerHTML = '<p>No results found.</p>';
            document.getElementById('paginationContainer').innerHTML = '';
        } else {
            //Sort results
            rankedResults.sort((a, b) => b.rank - a.rank);
            console.log(rankedResults);

            prevResults.set(query, rankedResults)

            displayResults(rankedResults, 1);
            setupPagination(rankedResults);
        }
    } catch (error) {
        document.getElementById('resultsContainer').innerHTML = '<p>Error loading results.</p>';
        console.error('Error performing search:', error);
    } finally {
        console.log("undisplay loading")
        loadingSpinner.style.display = 'none';
    }
    addQueryToQueue(query);
    updateSearchHistory();
}

function addQueryToQueue(query) {
    // Remove duplicates from the queue
    const existingIndex = previousSearchesQueue.indexOf(query);
    if (existingIndex !== -1) {
        previousSearchesQueue.splice(existingIndex, 1); // Remove the duplicate
    }
    // Add the new query to the end of the queue
    previousSearchesQueue.unshift(query);

    // If the queue exceeds the max size, remove the oldest query
    if (previousSearchesQueue.length > MAX_QUEUE_SIZE) {
        previousSearchesQueue.pop();
    }
}

function updateSearchHistory() {
    const searchHistoryContainer = document.getElementById('searchHistory');
    searchHistoryContainer.innerHTML = ''; // Clear the current list

    // Add each query in the queue to the sidebar as a list item
    previousSearchesQueue.forEach(query => {
        const listItem = document.createElement('li');
        listItem.textContent = query;

        // Make previous searches clickable to re-search
        listItem.onclick = () => {
            document.getElementById('searchInput').value = query;
            performSearch();
        };

        searchHistoryContainer.appendChild(listItem);
    });
}

function displayResults(results, page) {
    currentPage = page;
    const start = (page - 1) * resultsPerPage;
    const end = start + resultsPerPage;
    const paginatedResults = results.slice(start, end);

    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = paginatedResults.map(result => `<p>URL: <a href="${result.url}" target="_blank">${result.url}</a>, Score: ${result.rank}</p>`).join('');
}

function setupPagination(results) {
    const totalPages = Math.ceil(results.length / resultsPerPage);
    const paginationContainer = document.getElementById('paginationContainer');
    paginationContainer.innerHTML = '';

    for (let i = 1; i <= totalPages; i++) {
        const button = document.createElement('button');
        button.textContent = i;
        button.onclick = () => displayResults(results, i);
        if (i === currentPage) button.style.fontWeight = 'bold';
        paginationContainer.appendChild(button);
    }
}