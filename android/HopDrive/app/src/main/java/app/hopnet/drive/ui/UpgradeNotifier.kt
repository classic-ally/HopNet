package app.hopnet.drive.ui

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import app.hopnet.drive.BuildConfig
import app.hopnet.drive.MainActivity
import app.hopnet.drive.R
import app.hopnet.drive.data.UpgradeState
import app.hopnet.drive.net.formatVersionCode

/**
 * System-notification half of the upgrade-required surface. Lives behind
 * [UpgradeState] so the provider process — usually the first to see a 426,
 * with no activity running — can post it too. One fixed id: an episode is
 * a single notification, updated in place and cancelled on clear.
 */
object UpgradeNotifier {

    private const val CHANNEL_ID = "upgrade_required"
    const val NOTIFICATION_ID = 426

    fun show(context: Context, info: UpgradeState.Info) {
        // Silent skip when not permitted: the in-app banner remains the
        // guaranteed signal; the notification is best-effort.
        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) !=
            PackageManager.PERMISSION_GRANTED
        ) {
            return
        }
        val manager = context.getSystemService(NotificationManager::class.java)
        manager.createNotificationChannel(
            NotificationChannel(
                CHANNEL_ID,
                "Upgrade required",
                NotificationManager.IMPORTANCE_DEFAULT
            )
        )
        val tap = PendingIntent.getActivity(
            context,
            0,
            Intent(context, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("Update Hop Drive")
            .setContentText(
                "Node ${formatVersionCode(info.nodeVersion)} requires app " +
                    "${formatVersionCode(info.minClient)} or newer " +
                    "(installed: ${BuildConfig.HOPNET_CLIENT_VERSION_NAME})"
            )
            .setContentIntent(tap)
            .setAutoCancel(false)
            .build()
        manager.notify(NOTIFICATION_ID, notification)
    }

    fun cancel(context: Context) {
        context.getSystemService(NotificationManager::class.java).cancel(NOTIFICATION_ID)
    }
}
