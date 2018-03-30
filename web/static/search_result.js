function search(){
    var search_text = document.getElementById('search_text').value;
    window.location.href = '/search?keyword=' + search_text;
}

function getUrlParam(name) {
    var url = new URL(window.location.href);
    var result = url.searchParams.get(name);
    return result;
   }

function getSearchResult(){
    keyword = getUrlParam('keyword');
    var search_xhr = new XMLHttpRequest();
    search_xhr.open('GET', '/searchApi/' + keyword);
    search_xhr.send();
    search_xhr.onreadystatechange = function(){
        if(search_xhr.readyState == 4){
            if(search_xhr.status == 200){
                var result = JSON.parse(search_xhr.responseText);
                var search_text = document.getElementById('search_text');
                search_text.value = result['keyword'];
                var keyword_em = document.getElementById('keyword');
                keyword_em.innerHTML = result['keyword'];
                var search_result = document.createElement('div');
                search_result.className = 'search-result';
                for(var i = 0; i < result['lengths']; i++){
                    var a_search = document.createElement('div');
                    a_search.className = 'a-result';
                    var result_title = document.createElement('h3');
                    result_title.className = 'result-title';
                    var result_title_a = document.createElement('a');
                    result_title_a.href = result['urls'][i][0];
                    result_title_a.innerText = result['urls'][i][1];
                    var result_url = document.createElement('h4');
                    result_url.className = 'result-url';
                    result_url.innerText = result['urls'][i][0];
                    result_title.appendChild(result_title_a);
                    a_search.appendChild(result_title);
                    a_search.appendChild(result_url);
                    search_result.appendChild(a_search);
                }
                document.getElementsByTagName('body')[0].appendChild(search_result);
            }
        }
    }
}