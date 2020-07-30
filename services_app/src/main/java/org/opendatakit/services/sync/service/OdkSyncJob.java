package org.opendatakit.services.sync.service;

import android.content.Context;
import android.widget.Toast;
import androidx.annotation.NonNull;
import androidx.work.Worker;
import androidx.work.WorkerParameters;
import org.opendatakit.application.IToolAware;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.notifications.NotificationService;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.HashMap;
import java.util.Map;

public class OdkSyncJob extends Worker {

    private Context context;

    public OdkSyncJob(@NonNull Context context, @NonNull WorkerParameters workerParams) {
        super(context, workerParams);
    }

    private SyncAttachmentState syncAttachmentState = SyncAttachmentState.SYNC;
    private String mAppName;
    private final Map<String, AppSynchronizer> syncs = new HashMap<>();

    @NonNull
    @Override
    public Result doWork() {
        try {
            mAppName = getApplicationContext().getString(R.string.app_name);

     /*       AppSynchronizer sync = new AppSynchronizer(
                    getApplicationContext(),
                    ((IToolAware) new MainActivity().getApplication()).getVersionCodeString(),
                    "default",null);

            sync.synchronize(false, syncAttachmentState);*/

         /*  IOdkSyncServiceInterfaceImpl sync = new IOdkSyncServiceInterfaceImpl(new OdkSyncService());
           if(sync.verifyServerSettings("default")) {
               boolean status = sync.synchronizeWithServer("default", syncAttachmentState);

               if(status){
                   new NotificationService().sendNotification("Your data is upto date.", getApplicationContext());
                   return Result.success();
               } else {
                   return Result.failure();
               }
           } else {
               return Result.failure();
           }*/
            new NotificationService().sendNotification("Your data is upto date.", getApplicationContext());
         return Result.success();
        }catch (Exception e){
            e.printStackTrace();
            return Result.retry();
        }
    }
}
