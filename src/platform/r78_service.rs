// SPDX-License-Identifier: AGPL-3.0-only

//! Installed-service effects for the sign-in scope. The scope owns all
//! admission-dependent resources; this backend retains none of them.

use super::{
    hub_user_process::AdmittedUserHub,
    r78_lifecycle::{ReadinessBounds, ServiceBudgets, ServiceEffects, ServiceFailure},
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

pub struct InstalledServiceEffects {
    data_dir: PathBuf,
    budgets: ServiceBudgets,
    observed_loaded: Option<bool>,
}

impl InstalledServiceEffects {
    pub fn new(data_dir: &Path, budgets: ServiceBudgets) -> Self {
        Self {
            data_dir: data_dir.to_owned(),
            budgets,
            observed_loaded: None,
        }
    }
}

impl ServiceEffects for InstalledServiceEffects {
    type Admission = Arc<AdmittedUserHub>;
    type Error = io::Error;

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn loaded(&mut self, deadline: Instant) -> io::Result<bool> {
        #[cfg(feature = "credential-relocation-fixture")]
        let loaded = {
            require_time(deadline.min(Instant::now() + self.budgets.command))?;
            super::r78_recording::with_fixture_backend(|backend| backend.loaded_query())?
        };
        #[cfg(all(target_os = "macos", not(feature = "credential-relocation-fixture")))]
        let loaded = super::macos_launch_agent::service_is_loaded_until(deadline, self.budgets)?;
        #[cfg(all(
            not(target_os = "macos"),
            not(feature = "credential-relocation-fixture")
        ))]
        let loaded = {
            require_time(deadline.min(Instant::now() + self.budgets.command))?;
            false
        };
        if self.observed_loaded.is_none() {
            self.observed_loaded = Some(loaded);
        }
        Ok(loaded)
    }

    fn bootout(&mut self, deadline: Instant) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::transient_bootout_until(deadline, self.budgets)
        }
        #[cfg(not(target_os = "macos"))]
        {
            require_time(deadline)?;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "installed service unavailable",
            ))
        }
    }

    fn wait_unloaded(&mut self, deadline: Instant) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::wait_for_service_unloaded_until(deadline, self.budgets)
        }
        #[cfg(not(target_os = "macos"))]
        {
            require_time(deadline)
        }
    }

    fn admit(&mut self, deadline: Instant) -> io::Result<Self::Admission> {
        require_time(deadline)?;
        // An absent job creates no restart duty and permits one attempt only.
        if self.observed_loaded != Some(true) {
            return AdmittedUserHub::admit(&self.data_dir).map_err(io::Error::other);
        }
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::admit_after_service_stop_until(
                &self.data_dir,
                deadline,
                self.budgets,
            )
        }
        #[cfg(not(target_os = "macos"))]
        {
            AdmittedUserHub::admit(&self.data_dir).map_err(io::Error::other)
        }
    }

    fn bootstrap(&mut self, deadline: Instant) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::bootstrap_installed_until(deadline, self.budgets)
        }
        #[cfg(not(target_os = "macos"))]
        {
            require_time(deadline)?;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "installed service unavailable",
            ))
        }
    }

    fn kickstart(&mut self, deadline: Instant) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::kickstart_installed_until(deadline, self.budgets)
        }
        #[cfg(not(target_os = "macos"))]
        {
            require_time(deadline)?;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "installed service unavailable",
            ))
        }
    }

    fn wait_ready(&mut self, deadline: Instant, bounds: ReadinessBounds) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            super::macos_launch_agent::wait_for_current_installed_readiness_until(
                deadline,
                self.budgets,
                bounds,
            )
        }
        #[cfg(not(target_os = "macos"))]
        {
            require_time(deadline)?;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "installed service unavailable",
            ))
        }
    }

    fn record_unwind_restart_failure(&mut self, failure: ServiceFailure<Self::Error>) {
        drop(failure);
    }
}

pub(crate) fn require_time(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        Err(io::Error::new(io::ErrorKind::TimedOut, "service deadline"))
    } else {
        Ok(())
    }
}

/// A local helper has no pipe which a hung process can fill. Output is bounded
/// when read, and every timeout kills and waits for the exact child we spawned.
pub(crate) fn run_helper_until(
    program: &Path,
    arguments: &[&std::ffi::OsStr],
    deadline: Instant,
) -> io::Result<std::process::Output> {
    #[cfg(feature = "credential-relocation-fixture")]
    {
        let _ = (program, arguments, deadline);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "fixture_child_refused",
        ))
    }
    #[cfg(not(feature = "credential-relocation-fixture"))]
    {
        run_spawned_helper_until(program, arguments, deadline)
    }
}

#[cfg(not(feature = "credential-relocation-fixture"))]
fn run_spawned_helper_until(
    program: &Path,
    arguments: &[&std::ffi::OsStr],
    deadline: Instant,
) -> io::Result<std::process::Output> {
    {
        use std::{
            fs::File,
            io::{Read, Seek, SeekFrom},
            process::{Command, Stdio},
            time::Duration,
        };
        require_time(deadline)?;
        let mut stdout = tempfile::tempfile()?;
        let mut stderr = tempfile::tempfile()?;
        let mut child = Command::new(program)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(stdout.try_clone()?)
            .stderr(stderr.try_clone()?)
            .spawn()?;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error);
                }
            }
            if Instant::now() >= deadline {
                let kill = child.kill();
                let reap = child.wait();
                reap?;
                if let Err(error) = kill
                    && error.kind() != io::ErrorKind::InvalidInput
                {
                    return Err(error);
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "service helper deadline",
                ));
            }
            std::thread::sleep(
                Duration::from_millis(2).min(deadline.saturating_duration_since(Instant::now())),
            );
        };
        fn read_output(file: &mut File) -> io::Result<Vec<u8>> {
            file.seek(SeekFrom::Start(0))?;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(io::Error::other("service helper output cap"));
            }
            Ok(bytes)
        }
        require_time(deadline)?;
        Ok(std::process::Output {
            status,
            stdout: read_output(&mut stdout)?,
            stderr: read_output(&mut stderr)?,
        })
    }
}

#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn sleeping_helper_for_test(deadline: Instant) -> io::Result<()> {
    // CT100(q) runs this from inside a test hook, which holds the thread's hook slot,
    // so the hung helper is spawned directly rather than routed through the hook again.
    #[cfg(feature = "credential-relocation-fixture")]
    {
        let _ = deadline;
        Err(io::Error::new(io::ErrorKind::Unsupported, "fixture_child_refused"))
    }
    #[cfg(not(feature = "credential-relocation-fixture"))]
    {
        run_spawned_helper_until(
            Path::new("/bin/sleep"),
            &[std::ffi::OsStr::new("30")],
            deadline,
        )
        .map(|_| ())
    }
}
