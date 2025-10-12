use trpl::Html;

async fn page_title(url: &str) -> Option<String> {
    let response = trpl::get(url).await;
    let response_text = response.text().await;
    Html::parse(&response_text)
        .select_first("title")
        .map(|title| title.inner_html())
}


fn main() {
    trpl::run(async {
        let url = "https://www.rust-lang.org";
        match page_title(url).await {
            Some(title) => println!("The title of {url} was {title}"),
            None => println!("{url} had no title")
        }
    })
}
