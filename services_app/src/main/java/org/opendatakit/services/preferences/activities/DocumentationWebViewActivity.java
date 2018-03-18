package org.opendatakit.services.preferences.activities;


import android.app.Activity;
import android.os.Bundle;
import android.webkit.WebView;

import org.opendatakit.services.R;

public class DocumentationWebViewActivity extends Activity {
  private WebView webView;

  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.webview);
    webView = findViewById(R.id.documentationWebView);
    webView.getSettings().setJavaScriptEnabled(true);
    webView.loadUrl(getString(R.string.opendatakit_url));
  }
}
