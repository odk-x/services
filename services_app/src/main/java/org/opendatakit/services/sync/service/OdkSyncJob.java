package org.opendatakit.services.sync.service;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.Toast;
import androidx.annotation.NonNull;
import androidx.work.Worker;
import androidx.work.WorkerParameters;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.notifications.NotificationService;
import org.opendatakit.sync.service.SyncAttachmentState;

public class OdkSyncJob extends Worker {

    private Context context;
    Handler handler;

    public OdkSyncJob(@NonNull Context context, @NonNull WorkerParameters workerParams) {
        super(context, workerParams);
    }

    private SyncAttachmentState syncAttachmentState = SyncAttachmentState.SYNC;

    @NonNull
    @Override
    public Result doWork() {
        try {
            handler = new Handler(Looper.getMainLooper());

            new Thread(new Runnable() {
                @Override
                public void run() {

                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        handler.postDelayed(new Runnable() {
                            @Override public void run() {
                                Log.i("SYNC: ","processing...");
                                new MainActivity().performSync(syncAttachmentState);
                            }
                        }, 100);
                }
            }).start();

            Log.i("SYNC: ","Background sync SUCCESS !");
            return Result.success();
        }catch (Exception e){
            e.printStackTrace();
            new NotificationService().sendNotification("Background Sync failed", getApplicationContext());
            Log.e("Background Sync Error:", e.toString());
            return Result.retry();
        }
    }
}
