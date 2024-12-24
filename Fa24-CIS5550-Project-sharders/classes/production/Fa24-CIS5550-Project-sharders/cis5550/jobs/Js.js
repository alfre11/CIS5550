
    // Simulated dataset (replace this with actual search results from your backend)
    const dataset = Array.from({ length: 50 }, (_, i) => `Result ${i + 1}`);

    const previousSearchesQueue = [];
    const MAX_QUEUE_SIZE = 10;


    const resultsPerPage = 5; // Number of results per page
    let currentPage = 1;

    function performSearch() {
    const query = document.getElementById('searchInput').value.trim();
    if (!query) {
    alert('Please enter a search term.');
    return;
}

    addQueryToQueue(query);
    updateSearchHistory();

    const filteredResults = dataset.filter(item =>
    item.toLowerCase().includes(query.toLowerCase())
    );

    if (filteredResults.length === 0) {
    document.getElementById('resultsContainer').innerHTML = '<p>No results found.</p>';
    document.getElementById('paginationContainer').innerHTML = '';
} else {
        displayResults(filteredResults, 1); // Show first page of results
        setupPagination(filteredResults);
    }
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


    function iR() {
    const query = document.getElementById('searchInput').value.trim();
    const spl = query.split(" ");
    fetch()
    //

}

    function displayResults(results, page) {
    currentPage = page;
    const start = (page - 1) * resultsPerPage;
    const end = start + resultsPerPage;
    const paginatedResults = results.slice(start, end);

    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = paginatedResults.map(result => `<p>${result}</p>`).join('');
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