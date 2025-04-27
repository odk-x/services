package org.opendatakit.services;

import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.widget.ImageView;
import android.widget.LinearLayout;

import androidx.appcompat.app.AppCompatActivity;

import com.bumptech.glide.Glide;

public class SplashScreenActivity extends AppCompatActivity {

    private static final int LOGO_DISPLAY_TIME  = 500;
    private static final int GIF_DISPLAY_TIME  = 10000;
    private Handler handler = new Handler();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.splash_screen);

        ImageView iv = findViewById(R.id.splash);
        LinearLayout ll = findViewById(R.id.splash_layout);
        ImageView gif = findViewById(R.id.gif);


        handler.postDelayed(() -> {
            iv.setVisibility(ImageView.GONE); // Hide the logo
            ll.setVisibility(LinearLayout.VISIBLE);

            gif.setVisibility(ImageView.VISIBLE);
            Glide.with(SplashScreenActivity.this)
                    .asGif()
                    .load(R.drawable.settings_gif) // Replace with your actual GIF resource
                    .into(gif);
        }, LOGO_DISPLAY_TIME);


        handler.postDelayed(() -> {
            Intent intent = new Intent(SplashScreenActivity.this, MainActivity.class);
            startActivity(intent);
            finish();
        }, LOGO_DISPLAY_TIME + GIF_DISPLAY_TIME); // Add the display time of the logo
    }
}
