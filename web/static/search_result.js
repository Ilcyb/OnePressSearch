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

                    var result_content_p = document.createElement('p');
                    result_content_p.className = 'result-content'
                    result_content_p.innerHTML = red_keyword(result['urls'][i][2], result['keyword_list']);

                    var result_url = document.createElement('h4');
                    result_url.className = 'result-url';
                    result_url.innerText = result['urls'][i][0];
                    result_title.appendChild(result_title_a);
                    a_search.appendChild(result_title);
                    a_search.appendChild(result_content_p);
                    a_search.appendChild(result_url);
                    search_result.appendChild(a_search);
                }
                document.getElementsByTagName('body')[0].appendChild(search_result);
            }
        }
    }
}

function insert(str,flg,sn){
    var start = str.substr(0,sn);
    var end = str.substr(sn,str.length);
      var newstr = start+flg+end;
    return newstr;
}

function red_keyword(content, keywords){
    for(var i=0;i<keywords.length;i++){
        var begin = content.indexOf(keywords[i]);
        if(begin == -1)
            continue;
        end = begin + keywords[i].length + 22;
        content = insert(content, "<span class='keyword'>", begin);
        content = insert(content, "</span>", end);
    }
    return content;
}