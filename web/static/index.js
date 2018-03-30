function search(){
    var search_text = document.getElementById('search_text').value;
    window.location.href = '/search?keyword=' + search_text;
}