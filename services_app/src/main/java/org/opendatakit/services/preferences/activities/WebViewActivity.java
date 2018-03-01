package org.opendatakit.services.preferences.activities;


import android.app.Activity;
import android.os.Bundle;
import android.webkit.WebView;

import org.opendatakit.services.R;

public class WebViewActivity extends Activity {
  private WebView webView;

  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.webview);

    webView = (WebView) findViewById(R.id.webView1);
    webView.getSettings().setJavaScriptEnabled(true);
    webView.loadUrl("http://opendatakit.org");
  }
}
