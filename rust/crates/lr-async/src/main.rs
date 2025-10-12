use std::time::Duration;

use trpl::Html;

async fn page_title(url: &str) -> Option<String> {
    let response = trpl::get(url).await;
    let response_text = response.text().await;
    Html::parse(&response_text)
        .select_first("title")
        .map(|title| title.inner_html())
}

async fn count() {
    let handle = trpl::spawn_task(async {
        for i in 1..10 {
            println!("hi number {i} from the first task!");
            trpl::sleep(Duration::from_millis(500)).await;
        }
    });

    for i in 1..5 {
        println!("hi number {i} from the second task!");
        trpl::sleep(Duration::from_millis(500)).await;
    }

    handle.await.unwrap()
}

async fn message() {
    let (tx, mut rx) = trpl::channel();
    let vals = vec![
        String::from("hi"),
        String::from("from"),
        String::from("the"),
        String::from("future"),
    ];

    for val in vals {
        println!("sending {val}");
        tx.send(val).unwrap();
        trpl::sleep(Duration::from_millis(500)).await;
    }

    while let Some(value) = rx.recv().await {
        println!("received {value}");
    }
}


fn main() {
    trpl::run(async {
        message().await;
    })
}
