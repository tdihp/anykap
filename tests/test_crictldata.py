
from unittest.mock import Mock, patch
from anykap import *

CRICTL_PODS_OUTCOME = r'''
{
  "items": [
    {
      "id": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
      "metadata": {
        "name": "coredns-5d78c9869d-79bfv",
        "uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
        "namespace": "kube-system",
        "attempt": 0
      },
      "state": "SANDBOX_READY",
      "createdAt": "1706583754011264177",
      "labels": {
        "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
        "k8s-app": "kube-dns",
        "pod-template-hash": "5d78c9869d"
      },
      "annotations": {
        "kubernetes.io/config.seen": "2024-01-30T03:02:33.440020772Z",
        "kubernetes.io/config.source": "api"
      },
      "runtimeHandler": ""
    },
    {
      "id": "d4b9cb85ac28cd56d9b7a3b559fc41690abac8536b12ae5e1ede3c77b298600c",
      "metadata": {
        "name": "coredns-5d78c9869d-rbg8v",
        "uid": "da4c2d05-f96d-4a8f-96b5-da359a00a55e",
        "namespace": "kube-system",
        "attempt": 0
      },
      "state": "SANDBOX_READY",
      "createdAt": "1706583753794118510",
      "labels": {
        "io.kubernetes.pod.name": "coredns-5d78c9869d-rbg8v",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "da4c2d05-f96d-4a8f-96b5-da359a00a55e",
        "k8s-app": "kube-dns",
        "pod-template-hash": "5d78c9869d"
      },
      "annotations": {
        "kubernetes.io/config.seen": "2024-01-30T03:02:33.433758857Z",
        "kubernetes.io/config.source": "api"
      },
      "runtimeHandler": ""
    }
  ]
}
'''

CRICTL_PS_OUTCOME = r'''
{
  "containers": [
    {
      "id": "f3a240d625b1e97b859e29f81360fd79538021c46eaafb325dc0fa195c8e4b6a",
      "podSandboxId": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
      "metadata": {
        "name": "coredns",
        "attempt": 0
      },
      "image": {
        "image": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc",
        "annotations": {
        }
      },
      "imageRef": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc",
      "state": "CONTAINER_RUNNING",
      "createdAt": "1706583756562220088",
      "labels": {
        "io.kubernetes.container.name": "coredns",
        "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
      },
      "annotations": {
        "io.kubernetes.container.hash": "9d426e66",
        "io.kubernetes.container.ports": "[{\"name\":\"dns\",\"containerPort\":53,\"protocol\":\"UDP\"},{\"name\":\"dns-tcp\",\"containerPort\":53,\"protocol\":\"TCP\"},{\"name\":\"metrics\",\"containerPort\":9153,\"protocol\":\"TCP\"}]",
        "io.kubernetes.container.restartCount": "0",
        "io.kubernetes.container.terminationMessagePath": "/dev/termination-log",
        "io.kubernetes.container.terminationMessagePolicy": "File",
        "io.kubernetes.pod.terminationGracePeriod": "30"
      }
    },
    {
      "id": "5fbcaeab34f4da1a229c5b4c823449ee6ec41288434a3ca7167291dbf3731f7a",
      "podSandboxId": "d4b9cb85ac28cd56d9b7a3b559fc41690abac8536b12ae5e1ede3c77b298600c",
      "metadata": {
        "name": "coredns",
        "attempt": 0
      },
      "image": {
        "image": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc",
        "annotations": {
        }
      },
      "imageRef": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc",
      "state": "CONTAINER_RUNNING",
      "createdAt": "1706583756397578325",
      "labels": {
        "io.kubernetes.container.name": "coredns",
        "io.kubernetes.pod.name": "coredns-5d78c9869d-rbg8v",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "da4c2d05-f96d-4a8f-96b5-da359a00a55e"
      },
      "annotations": {
        "io.kubernetes.container.hash": "78c3543d",
        "io.kubernetes.container.ports": "[{\"name\":\"dns\",\"containerPort\":53,\"protocol\":\"UDP\"},{\"name\":\"dns-tcp\",\"containerPort\":53,\"protocol\":\"TCP\"},{\"name\":\"metrics\",\"containerPort\":9153,\"protocol\":\"TCP\"}]",
        "io.kubernetes.container.restartCount": "0",
        "io.kubernetes.container.terminationMessagePath": "/dev/termination-log",
        "io.kubernetes.container.terminationMessagePolicy": "File",
        "io.kubernetes.pod.terminationGracePeriod": "30"
      }
    }
  ]
}
'''

CRICTL_IMG_OUTCOME = r'''
{
  "images": [
    {
      "id": "sha256:b0b1fa0f58c6e932b7f20bf208b2841317a1e8c88cc51b18358310bbd8ec95da",
      "repoTags": [
        "docker.io/kindest/kindnetd:v20230511-dc714da8"
      ],
      "repoDigests": [
      ],
      "size": "27731571",
      "uid": null,
      "username": "",
      "spec": null,
      "pinned": false
    },
    {
      "id": "sha256:be300acfc86223548b4949398f964389b7309dfcfdcfc89125286359abb86956",
      "repoTags": [
        "docker.io/kindest/local-path-helper:v20230510-486859a6"
      ],
      "repoDigests": [
      ],
      "size": "3052318",
      "uid": {
        "value": "0"
      },
      "username": "",
      "spec": null,
      "pinned": false
    }
  ]
}
'''

CRICTL_INSPECTP_OUTCOME = r'''
{
  "status": {
    "id": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
    "metadata": {
      "attempt": 0,
      "name": "coredns-5d78c9869d-79bfv",
      "namespace": "kube-system",
      "uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
    },
    "state": "SANDBOX_READY",
    "createdAt": "2024-01-30T03:02:34.011264177Z",
    "network": {
      "additionalIps": [],
      "ip": "10.244.0.3"
    },
    "linux": {
      "namespaces": {
        "options": {
          "ipc": "POD",
          "network": "POD",
          "pid": "CONTAINER",
          "targetId": "",
          "usernsOptions": null
        }
      }
    },
    "labels": {
      "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
      "io.kubernetes.pod.namespace": "kube-system",
      "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
      "k8s-app": "kube-dns",
      "pod-template-hash": "5d78c9869d"
    },
    "annotations": {
      "kubernetes.io/config.seen": "2024-01-30T03:02:33.440020772Z",
      "kubernetes.io/config.source": "api"
    },
    "runtimeHandler": ""
  },
  "info": {
    "pid": 1322,
    "processStatus": "running",
    "netNamespaceClosed": false,
    "image": "registry.k8s.io/pause:3.7",
    "snapshotKey": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
    "snapshotter": "overlayfs",
    "runtimeHandler": "",
    "runtimeType": "io.containerd.runc.v2",
    "runtimeOptions": {
      "systemd_cgroup": true
    },
    "config": {
      "metadata": {
        "name": "coredns-5d78c9869d-79bfv",
        "uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
        "namespace": "kube-system"
      },
      "hostname": "coredns-5d78c9869d-79bfv",
      "log_directory": "/var/log/pods/kube-system_coredns-5d78c9869d-79bfv_44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
      "dns_config": {
        "servers": [
          "192.168.65.254"
        ],
        "options": [
          "ndots:0"
        ]
      },
      "port_mappings": [
        {
          "protocol": 1,
          "container_port": 53
        },
        {
          "container_port": 53
        },
        {
          "container_port": 9153
        }
      ],
      "labels": {
        "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
        "k8s-app": "kube-dns",
        "pod-template-hash": "5d78c9869d"
      },
      "annotations": {
        "kubernetes.io/config.seen": "2024-01-30T03:02:33.440020772Z",
        "kubernetes.io/config.source": "api"
      },
      "linux": {
        "cgroup_parent": "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice/kubelet-kubepods-burstable-pod44e4b37f_504f_4f1c_80f3_4137bfbec5e4.slice",
        "security_context": {
          "namespace_options": {
            "pid": 1
          },
          "seccomp": {}
        },
        "overhead": {},
        "resources": {
          "cpu_period": 100000,
          "cpu_shares": 102,
          "memory_limit_in_bytes": 178257920
        }
      }
    },
    "runtimeSpec": {
      "ociVersion": "1.1.0-rc.1",
      "process": {
        "user": {
          "uid": 65535,
          "gid": 65535,
          "additionalGids": [
            65535
          ]
        },
        "args": [
          "/pause"
        ],
        "env": [
          "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        ],
        "cwd": "/",
        "capabilities": {
          "bounding": [
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FSETID",
            "CAP_FOWNER",
            "CAP_MKNOD",
            "CAP_NET_RAW",
            "CAP_SETGID",
            "CAP_SETUID",
            "CAP_SETFCAP",
            "CAP_SETPCAP",
            "CAP_NET_BIND_SERVICE",
            "CAP_SYS_CHROOT",
            "CAP_KILL",
            "CAP_AUDIT_WRITE"
          ],
          "effective": [
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FSETID",
            "CAP_FOWNER",
            "CAP_MKNOD",
            "CAP_NET_RAW",
            "CAP_SETGID",
            "CAP_SETUID",
            "CAP_SETFCAP",
            "CAP_SETPCAP",
            "CAP_NET_BIND_SERVICE",
            "CAP_SYS_CHROOT",
            "CAP_KILL",
            "CAP_AUDIT_WRITE"
          ],
          "permitted": [
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FSETID",
            "CAP_FOWNER",
            "CAP_MKNOD",
            "CAP_NET_RAW",
            "CAP_SETGID",
            "CAP_SETUID",
            "CAP_SETFCAP",
            "CAP_SETPCAP",
            "CAP_NET_BIND_SERVICE",
            "CAP_SYS_CHROOT",
            "CAP_KILL",
            "CAP_AUDIT_WRITE"
          ]
        },
        "noNewPrivileges": true,
        "oomScoreAdj": -998
      },
      "root": {
        "path": "rootfs",
        "readonly": true
      },
      "hostname": "coredns-5d78c9869d-79bfv",
      "mounts": [
        {
          "destination": "/proc",
          "type": "proc",
          "source": "proc",
          "options": [
            "nosuid",
            "noexec",
            "nodev"
          ]
        },
        {
          "destination": "/dev",
          "type": "tmpfs",
          "source": "tmpfs",
          "options": [
            "nosuid",
            "strictatime",
            "mode=755",
            "size=65536k"
          ]
        },
        {
          "destination": "/dev/pts",
          "type": "devpts",
          "source": "devpts",
          "options": [
            "nosuid",
            "noexec",
            "newinstance",
            "ptmxmode=0666",
            "mode=0620",
            "gid=5"
          ]
        },
        {
          "destination": "/dev/mqueue",
          "type": "mqueue",
          "source": "mqueue",
          "options": [
            "nosuid",
            "noexec",
            "nodev"
          ]
        },
        {
          "destination": "/sys",
          "type": "sysfs",
          "source": "sysfs",
          "options": [
            "nosuid",
            "noexec",
            "nodev",
            "ro"
          ]
        },
        {
          "destination": "/dev/shm",
          "type": "bind",
          "source": "/run/containerd/io.containerd.grpc.v1.cri/sandboxes/3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535/shm",
          "options": [
            "rbind",
            "ro",
            "nosuid",
            "nodev",
            "noexec"
          ]
        },
        {
          "destination": "/etc/resolv.conf",
          "type": "bind",
          "source": "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535/resolv.conf",
          "options": [
            "rbind",
            "ro",
            "nosuid",
            "nodev",
            "noexec"
          ]
        }
      ],
      "annotations": {
        "io.kubernetes.cri.container-type": "sandbox",
        "io.kubernetes.cri.sandbox-cpu-period": "100000",
        "io.kubernetes.cri.sandbox-cpu-quota": "0",
        "io.kubernetes.cri.sandbox-cpu-shares": "102",
        "io.kubernetes.cri.sandbox-id": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
        "io.kubernetes.cri.sandbox-log-directory": "/var/log/pods/kube-system_coredns-5d78c9869d-79bfv_44e4b37f-504f-4f1c-80f3-4137bfbec5e4",
        "io.kubernetes.cri.sandbox-memory": "178257920",
        "io.kubernetes.cri.sandbox-name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.cri.sandbox-namespace": "kube-system",
        "io.kubernetes.cri.sandbox-uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
      },
      "linux": {
        "resources": {
          "devices": [
            {
              "allow": false,
              "access": "rwm"
            }
          ],
          "cpu": {
            "shares": 2
          }
        },
        "cgroupsPath": "kubelet-kubepods-burstable-pod44e4b37f_504f_4f1c_80f3_4137bfbec5e4.slice:cri-containerd:3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
        "namespaces": [
          {
            "type": "pid"
          },
          {
            "type": "ipc"
          },
          {
            "type": "uts"
          },
          {
            "type": "mount"
          },
          {
            "type": "network",
            "path": "/var/run/netns/cni-c8f12417-13bc-a776-937a-0d987bcf8a2a"
          }
        ],
        "seccomp": {
          "defaultAction": "SCMP_ACT_ERRNO",
          "architectures": [
            "SCMP_ARCH_X86_64",
            "SCMP_ARCH_X86",
            "SCMP_ARCH_X32"
          ],
          "syscalls": [
            {
              "names": [
                "accept",
                "accept4",
                "access",
                "adjtimex",
                "alarm",
                "bind",
                "brk",
                "capget",
                "capset",
                "chdir",
                "chmod",
                "chown",
                "chown32",
                "clock_adjtime",
                "clock_adjtime64",
                "clock_getres",
                "clock_getres_time64",
                "clock_gettime",
                "clock_gettime64",
                "clock_nanosleep",
                "clock_nanosleep_time64",
                "close",
                "close_range",
                "connect",
                "copy_file_range",
                "creat",
                "dup",
                "dup2",
                "dup3",
                "epoll_create",
                "epoll_create1",
                "epoll_ctl",
                "epoll_ctl_old",
                "epoll_pwait",
                "epoll_pwait2",
                "epoll_wait",
                "epoll_wait_old",
                "eventfd",
                "eventfd2",
                "execve",
                "execveat",
                "exit",
                "exit_group",
                "faccessat",
                "faccessat2",
                "fadvise64",
                "fadvise64_64",
                "fallocate",
                "fanotify_mark",
                "fchdir",
                "fchmod",
                "fchmodat",
                "fchown",
                "fchown32",
                "fchownat",
                "fcntl",
                "fcntl64",
                "fdatasync",
                "fgetxattr",
                "flistxattr",
                "flock",
                "fork",
                "fremovexattr",
                "fsetxattr",
                "fstat",
                "fstat64",
                "fstatat64",
                "fstatfs",
                "fstatfs64",
                "fsync",
                "ftruncate",
                "ftruncate64",
                "futex",
                "futex_time64",
                "futex_waitv",
                "futimesat",
                "getcpu",
                "getcwd",
                "getdents",
                "getdents64",
                "getegid",
                "getegid32",
                "geteuid",
                "geteuid32",
                "getgid",
                "getgid32",
                "getgroups",
                "getgroups32",
                "getitimer",
                "getpeername",
                "getpgid",
                "getpgrp",
                "getpid",
                "getppid",
                "getpriority",
                "getrandom",
                "getresgid",
                "getresgid32",
                "getresuid",
                "getresuid32",
                "getrlimit",
                "get_robust_list",
                "getrusage",
                "getsid",
                "getsockname",
                "getsockopt",
                "get_thread_area",
                "gettid",
                "gettimeofday",
                "getuid",
                "getuid32",
                "getxattr",
                "inotify_add_watch",
                "inotify_init",
                "inotify_init1",
                "inotify_rm_watch",
                "io_cancel",
                "ioctl",
                "io_destroy",
                "io_getevents",
                "io_pgetevents",
                "io_pgetevents_time64",
                "ioprio_get",
                "ioprio_set",
                "io_setup",
                "io_submit",
                "io_uring_enter",
                "io_uring_register",
                "io_uring_setup",
                "ipc",
                "kill",
                "landlock_add_rule",
                "landlock_create_ruleset",
                "landlock_restrict_self",
                "lchown",
                "lchown32",
                "lgetxattr",
                "link",
                "linkat",
                "listen",
                "listxattr",
                "llistxattr",
                "_llseek",
                "lremovexattr",
                "lseek",
                "lsetxattr",
                "lstat",
                "lstat64",
                "madvise",
                "membarrier",
                "memfd_create",
                "memfd_secret",
                "mincore",
                "mkdir",
                "mkdirat",
                "mknod",
                "mknodat",
                "mlock",
                "mlock2",
                "mlockall",
                "mmap",
                "mmap2",
                "mprotect",
                "mq_getsetattr",
                "mq_notify",
                "mq_open",
                "mq_timedreceive",
                "mq_timedreceive_time64",
                "mq_timedsend",
                "mq_timedsend_time64",
                "mq_unlink",
                "mremap",
                "msgctl",
                "msgget",
                "msgrcv",
                "msgsnd",
                "msync",
                "munlock",
                "munlockall",
                "munmap",
                "nanosleep",
                "newfstatat",
                "_newselect",
                "open",
                "openat",
                "openat2",
                "pause",
                "pidfd_open",
                "pidfd_send_signal",
                "pipe",
                "pipe2",
                "pkey_alloc",
                "pkey_free",
                "pkey_mprotect",
                "poll",
                "ppoll",
                "ppoll_time64",
                "prctl",
                "pread64",
                "preadv",
                "preadv2",
                "prlimit64",
                "process_mrelease",
                "pselect6",
                "pselect6_time64",
                "pwrite64",
                "pwritev",
                "pwritev2",
                "read",
                "readahead",
                "readlink",
                "readlinkat",
                "readv",
                "recv",
                "recvfrom",
                "recvmmsg",
                "recvmmsg_time64",
                "recvmsg",
                "remap_file_pages",
                "removexattr",
                "rename",
                "renameat",
                "renameat2",
                "restart_syscall",
                "rmdir",
                "rseq",
                "rt_sigaction",
                "rt_sigpending",
                "rt_sigprocmask",
                "rt_sigqueueinfo",
                "rt_sigreturn",
                "rt_sigsuspend",
                "rt_sigtimedwait",
                "rt_sigtimedwait_time64",
                "rt_tgsigqueueinfo",
                "sched_getaffinity",
                "sched_getattr",
                "sched_getparam",
                "sched_get_priority_max",
                "sched_get_priority_min",
                "sched_getscheduler",
                "sched_rr_get_interval",
                "sched_rr_get_interval_time64",
                "sched_setaffinity",
                "sched_setattr",
                "sched_setparam",
                "sched_setscheduler",
                "sched_yield",
                "seccomp",
                "select",
                "semctl",
                "semget",
                "semop",
                "semtimedop",
                "semtimedop_time64",
                "send",
                "sendfile",
                "sendfile64",
                "sendmmsg",
                "sendmsg",
                "sendto",
                "setfsgid",
                "setfsgid32",
                "setfsuid",
                "setfsuid32",
                "setgid",
                "setgid32",
                "setgroups",
                "setgroups32",
                "setitimer",
                "setpgid",
                "setpriority",
                "setregid",
                "setregid32",
                "setresgid",
                "setresgid32",
                "setresuid",
                "setresuid32",
                "setreuid",
                "setreuid32",
                "setrlimit",
                "set_robust_list",
                "setsid",
                "setsockopt",
                "set_thread_area",
                "set_tid_address",
                "setuid",
                "setuid32",
                "setxattr",
                "shmat",
                "shmctl",
                "shmdt",
                "shmget",
                "shutdown",
                "sigaltstack",
                "signalfd",
                "signalfd4",
                "sigprocmask",
                "sigreturn",
                "socketcall",
                "socketpair",
                "splice",
                "stat",
                "stat64",
                "statfs",
                "statfs64",
                "statx",
                "symlink",
                "symlinkat",
                "sync",
                "sync_file_range",
                "syncfs",
                "sysinfo",
                "tee",
                "tgkill",
                "time",
                "timer_create",
                "timer_delete",
                "timer_getoverrun",
                "timer_gettime",
                "timer_gettime64",
                "timer_settime",
                "timer_settime64",
                "timerfd_create",
                "timerfd_gettime",
                "timerfd_gettime64",
                "timerfd_settime",
                "timerfd_settime64",
                "times",
                "tkill",
                "truncate",
                "truncate64",
                "ugetrlimit",
                "umask",
                "uname",
                "unlink",
                "unlinkat",
                "utime",
                "utimensat",
                "utimensat_time64",
                "utimes",
                "vfork",
                "vmsplice",
                "wait4",
                "waitid",
                "waitpid",
                "write",
                "writev"
              ],
              "action": "SCMP_ACT_ALLOW"
            },
            {
              "names": [
                "socket"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 40,
                  "op": "SCMP_CMP_NE"
                }
              ]
            },
            {
              "names": [
                "personality"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 0,
                  "op": "SCMP_CMP_EQ"
                }
              ]
            },
            {
              "names": [
                "personality"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 8,
                  "op": "SCMP_CMP_EQ"
                }
              ]
            },
            {
              "names": [
                "personality"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 131072,
                  "op": "SCMP_CMP_EQ"
                }
              ]
            },
            {
              "names": [
                "personality"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 131080,
                  "op": "SCMP_CMP_EQ"
                }
              ]
            },
            {
              "names": [
                "personality"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 4294967295,
                  "op": "SCMP_CMP_EQ"
                }
              ]
            },
            {
              "names": [
                "process_vm_readv",
                "process_vm_writev",
                "ptrace"
              ],
              "action": "SCMP_ACT_ALLOW"
            },
            {
              "names": [
                "arch_prctl",
                "modify_ldt"
              ],
              "action": "SCMP_ACT_ALLOW"
            },
            {
              "names": [
                "chroot"
              ],
              "action": "SCMP_ACT_ALLOW"
            },
            {
              "names": [
                "clone"
              ],
              "action": "SCMP_ACT_ALLOW",
              "args": [
                {
                  "index": 0,
                  "value": 2114060288,
                  "op": "SCMP_CMP_MASKED_EQ"
                }
              ]
            },
            {
              "names": [
                "clone3"
              ],
              "action": "SCMP_ACT_ERRNO",
              "errnoRet": 38
            }
          ]
        },
        "maskedPaths": [
          "/proc/acpi",
          "/proc/asound",
          "/proc/kcore",
          "/proc/keys",
          "/proc/latency_stats",
          "/proc/timer_list",
          "/proc/timer_stats",
          "/proc/sched_debug",
          "/sys/firmware",
          "/proc/scsi"
        ],
        "readonlyPaths": [
          "/proc/bus",
          "/proc/fs",
          "/proc/irq",
          "/proc/sys",
          "/proc/sysrq-trigger"
        ]
      }
    },
    "cniResult": {
      "Interfaces": {
        "eth0": {
          "IPConfigs": [
            {
              "IP": "10.244.0.3",
              "Gateway": "10.244.0.1"
            }
          ],
          "Mac": "da:83:53:a5:9e:9e",
          "Sandbox": "/var/run/netns/cni-c8f12417-13bc-a776-937a-0d987bcf8a2a"
        },
        "lo": {
          "IPConfigs": [
            {
              "IP": "127.0.0.1",
              "Gateway": ""
            },
            {
              "IP": "::1",
              "Gateway": ""
            }
          ],
          "Mac": "00:00:00:00:00:00",
          "Sandbox": "/var/run/netns/cni-c8f12417-13bc-a776-937a-0d987bcf8a2a"
        },
        "vethfac58f1c": {
          "IPConfigs": null,
          "Mac": "0e:d6:b9:08:62:54",
          "Sandbox": ""
        }
      },
      "DNS": [
        {},
        {}
      ],
      "Routes": [
        {
          "dst": "0.0.0.0/0"
        }
      ]
    }
  }
}
'''

CRICTL_INSPECT_OUTCOME = r'''
{
  "status": {
    "id": "f3a240d625b1e97b859e29f81360fd79538021c46eaafb325dc0fa195c8e4b6a",
    "metadata": {
      "attempt": 0,
      "name": "coredns"
    },
    "state": "CONTAINER_RUNNING",
    "createdAt": "2024-01-30T03:02:36.562220088Z",
    "startedAt": "2024-01-30T03:02:36.812479971Z",
    "finishedAt": "0001-01-01T00:00:00Z",
    "exitCode": 0,
    "image": {
      "annotations": {},
      "image": "registry.k8s.io/coredns/coredns:v1.10.1"
    },
    "imageRef": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc",
    "reason": "",
    "message": "",
    "labels": {
      "io.kubernetes.container.name": "coredns",
      "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
      "io.kubernetes.pod.namespace": "kube-system",
      "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
    },
    "annotations": {
      "io.kubernetes.container.hash": "9d426e66",
      "io.kubernetes.container.ports": "[{\"name\":\"dns\",\"containerPort\":53,\"protocol\":\"UDP\"},{\"name\":\"dns-tcp\",\"containerPort\":53,\"protocol\":\"TCP\"},{\"name\":\"metrics\",\"containerPort\":9153,\"protocol\":\"TCP\"}]",
      "io.kubernetes.container.restartCount": "0",
      "io.kubernetes.container.terminationMessagePath": "/dev/termination-log",
      "io.kubernetes.container.terminationMessagePolicy": "File",
      "io.kubernetes.pod.terminationGracePeriod": "30"
    },
    "mounts": [
      {
        "containerPath": "/etc/coredns",
        "gidMappings": [],
        "hostPath": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~configmap/config-volume",
        "propagation": "PROPAGATION_PRIVATE",
        "readonly": true,
        "selinuxRelabel": false,
        "uidMappings": []
      },
      {
        "containerPath": "/var/run/secrets/kubernetes.io/serviceaccount",
        "gidMappings": [],
        "hostPath": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~projected/kube-api-access-clt7l",
        "propagation": "PROPAGATION_PRIVATE",
        "readonly": true,
        "selinuxRelabel": false,
        "uidMappings": []
      },
      {
        "containerPath": "/etc/hosts",
        "gidMappings": [],
        "hostPath": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/etc-hosts",
        "propagation": "PROPAGATION_PRIVATE",
        "readonly": false,
        "selinuxRelabel": false,
        "uidMappings": []
      },
      {
        "containerPath": "/dev/termination-log",
        "gidMappings": [],
        "hostPath": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/containers/coredns/0db6db2a",
        "propagation": "PROPAGATION_PRIVATE",
        "readonly": false,
        "selinuxRelabel": false,
        "uidMappings": []
      }
    ],
    "logPath": "/var/log/pods/kube-system_coredns-5d78c9869d-79bfv_44e4b37f-504f-4f1c-80f3-4137bfbec5e4/coredns/0.log",
    "resources": {
      "linux": {
        "cpuPeriod": "100000",
        "cpuQuota": "0",
        "cpuShares": "102",
        "cpusetCpus": "",
        "cpusetMems": "",
        "hugepageLimits": [],
        "memoryLimitInBytes": "178257920",
        "memorySwapLimitInBytes": "178257920",
        "oomScoreAdj": "992",
        "unified": {}
      },
      "windows": null
    }
  },
  "info": {
    "sandboxID": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
    "pid": 1478,
    "removing": false,
    "snapshotKey": "f3a240d625b1e97b859e29f81360fd79538021c46eaafb325dc0fa195c8e4b6a",
    "snapshotter": "overlayfs",
    "runtimeType": "io.containerd.runc.v2",
    "runtimeOptions": {
      "systemd_cgroup": true
    },
    "config": {
      "metadata": {
        "name": "coredns"
      },
      "image": {
        "image": "sha256:ead0a4a53df89fd173874b46093b6e62d8c72967bbf606d672c9e8c9b601a4fc"
      },
      "args": [
        "-conf",
        "/etc/coredns/Corefile"
      ],
      "envs": [
        {
          "key": "KUBERNETES_PORT_443_TCP_ADDR",
          "value": "10.96.0.1"
        },
        {
          "key": "KUBE_DNS_SERVICE_PORT",
          "value": "53"
        },
        {
          "key": "KUBE_DNS_SERVICE_PORT_DNS",
          "value": "53"
        },
        {
          "key": "KUBE_DNS_PORT_53_TCP_PROTO",
          "value": "tcp"
        },
        {
          "key": "KUBE_DNS_PORT_53_TCP_ADDR",
          "value": "10.96.0.10"
        },
        {
          "key": "KUBE_DNS_PORT_9153_TCP_ADDR",
          "value": "10.96.0.10"
        },
        {
          "key": "KUBERNETES_SERVICE_HOST",
          "value": "10.96.0.1"
        },
        {
          "key": "KUBERNETES_SERVICE_PORT",
          "value": "443"
        },
        {
          "key": "KUBE_DNS_PORT_53_UDP",
          "value": "udp://10.96.0.10:53"
        },
        {
          "key": "KUBE_DNS_PORT_53_UDP_PORT",
          "value": "53"
        },
        {
          "key": "KUBE_DNS_PORT_53_TCP",
          "value": "tcp://10.96.0.10:53"
        },
        {
          "key": "KUBE_DNS_PORT_53_TCP_PORT",
          "value": "53"
        },
        {
          "key": "KUBE_DNS_PORT_9153_TCP",
          "value": "tcp://10.96.0.10:9153"
        },
        {
          "key": "KUBE_DNS_PORT_9153_TCP_PORT",
          "value": "9153"
        },
        {
          "key": "KUBERNETES_SERVICE_PORT_HTTPS",
          "value": "443"
        },
        {
          "key": "KUBERNETES_PORT",
          "value": "tcp://10.96.0.1:443"
        },
        {
          "key": "KUBERNETES_PORT_443_TCP_PORT",
          "value": "443"
        },
        {
          "key": "KUBE_DNS_PORT",
          "value": "udp://10.96.0.10:53"
        },
        {
          "key": "KUBERNETES_PORT_443_TCP",
          "value": "tcp://10.96.0.1:443"
        },
        {
          "key": "KUBERNETES_PORT_443_TCP_PROTO",
          "value": "tcp"
        },
        {
          "key": "KUBE_DNS_SERVICE_PORT_METRICS",
          "value": "9153"
        },
        {
          "key": "KUBE_DNS_PORT_53_UDP_PROTO",
          "value": "udp"
        },
        {
          "key": "KUBE_DNS_PORT_53_UDP_ADDR",
          "value": "10.96.0.10"
        },
        {
          "key": "KUBE_DNS_PORT_9153_TCP_PROTO",
          "value": "tcp"
        },
        {
          "key": "KUBE_DNS_SERVICE_HOST",
          "value": "10.96.0.10"
        },
        {
          "key": "KUBE_DNS_SERVICE_PORT_DNS_TCP",
          "value": "53"
        }
      ],
      "mounts": [
        {
          "container_path": "/etc/coredns",
          "host_path": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~configmap/config-volume",
          "readonly": true
        },
        {
          "container_path": "/var/run/secrets/kubernetes.io/serviceaccount",
          "host_path": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~projected/kube-api-access-clt7l",
          "readonly": true
        },
        {
          "container_path": "/etc/hosts",
          "host_path": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/etc-hosts"
        },
        {
          "container_path": "/dev/termination-log",
          "host_path": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/containers/coredns/0db6db2a"
        }
      ],
      "labels": {
        "io.kubernetes.container.name": "coredns",
        "io.kubernetes.pod.name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.pod.namespace": "kube-system",
        "io.kubernetes.pod.uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
      },
      "annotations": {
        "io.kubernetes.container.hash": "9d426e66",
        "io.kubernetes.container.ports": "[{\"name\":\"dns\",\"containerPort\":53,\"protocol\":\"UDP\"},{\"name\":\"dns-tcp\",\"containerPort\":53,\"protocol\":\"TCP\"},{\"name\":\"metrics\",\"containerPort\":9153,\"protocol\":\"TCP\"}]",
        "io.kubernetes.container.restartCount": "0",
        "io.kubernetes.container.terminationMessagePath": "/dev/termination-log",
        "io.kubernetes.container.terminationMessagePolicy": "File",
        "io.kubernetes.pod.terminationGracePeriod": "30"
      },
      "log_path": "coredns/0.log",
      "linux": {
        "resources": {
          "cpu_period": 100000,
          "cpu_shares": 102,
          "memory_limit_in_bytes": 178257920,
          "oom_score_adj": 992,
          "hugepage_limits": [
            {
              "page_size": "2MB"
            },
            {
              "page_size": "1GB"
            }
          ]
        },
        "security_context": {
          "capabilities": {
            "add_capabilities": [
              "NET_BIND_SERVICE"
            ],
            "drop_capabilities": [
              "all"
            ]
          },
          "namespace_options": {
            "pid": 1
          },
          "run_as_user": {},
          "readonly_rootfs": true,
          "no_new_privs": true,
          "masked_paths": [
            "/proc/asound",
            "/proc/acpi",
            "/proc/kcore",
            "/proc/keys",
            "/proc/latency_stats",
            "/proc/timer_list",
            "/proc/timer_stats",
            "/proc/sched_debug",
            "/proc/scsi",
            "/sys/firmware"
          ],
          "readonly_paths": [
            "/proc/bus",
            "/proc/fs",
            "/proc/irq",
            "/proc/sys",
            "/proc/sysrq-trigger"
          ],
          "seccomp": {
            "profile_type": 1
          }
        }
      }
    },
    "runtimeSpec": {
      "ociVersion": "1.1.0-rc.1",
      "process": {
        "user": {
          "uid": 0,
          "gid": 0,
          "additionalGids": [
            0
          ]
        },
        "args": [
          "/coredns",
          "-conf",
          "/etc/coredns/Corefile"
        ],
        "env": [
          "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
          "HOSTNAME=coredns-5d78c9869d-79bfv",
          "KUBERNETES_PORT_443_TCP_ADDR=10.96.0.1",
          "KUBE_DNS_SERVICE_PORT=53",
          "KUBE_DNS_SERVICE_PORT_DNS=53",
          "KUBE_DNS_PORT_53_TCP_PROTO=tcp",
          "KUBE_DNS_PORT_53_TCP_ADDR=10.96.0.10",
          "KUBE_DNS_PORT_9153_TCP_ADDR=10.96.0.10",
          "KUBERNETES_SERVICE_HOST=10.96.0.1",
          "KUBERNETES_SERVICE_PORT=443",
          "KUBE_DNS_PORT_53_UDP=udp://10.96.0.10:53",
          "KUBE_DNS_PORT_53_UDP_PORT=53",
          "KUBE_DNS_PORT_53_TCP=tcp://10.96.0.10:53",
          "KUBE_DNS_PORT_53_TCP_PORT=53",
          "KUBE_DNS_PORT_9153_TCP=tcp://10.96.0.10:9153",
          "KUBE_DNS_PORT_9153_TCP_PORT=9153",
          "KUBERNETES_SERVICE_PORT_HTTPS=443",
          "KUBERNETES_PORT=tcp://10.96.0.1:443",
          "KUBERNETES_PORT_443_TCP_PORT=443",
          "KUBE_DNS_PORT=udp://10.96.0.10:53",
          "KUBERNETES_PORT_443_TCP=tcp://10.96.0.1:443",
          "KUBERNETES_PORT_443_TCP_PROTO=tcp",
          "KUBE_DNS_SERVICE_PORT_METRICS=9153",
          "KUBE_DNS_PORT_53_UDP_PROTO=udp",
          "KUBE_DNS_PORT_53_UDP_ADDR=10.96.0.10",
          "KUBE_DNS_PORT_9153_TCP_PROTO=tcp",
          "KUBE_DNS_SERVICE_HOST=10.96.0.10",
          "KUBE_DNS_SERVICE_PORT_DNS_TCP=53"
        ],
        "cwd": "/",
        "capabilities": {
          "bounding": [
            "CAP_NET_BIND_SERVICE"
          ],
          "effective": [
            "CAP_NET_BIND_SERVICE"
          ],
          "permitted": [
            "CAP_NET_BIND_SERVICE"
          ]
        },
        "noNewPrivileges": true,
        "oomScoreAdj": 992
      },
      "root": {
        "path": "rootfs",
        "readonly": true
      },
      "mounts": [
        {
          "destination": "/proc",
          "type": "proc",
          "source": "proc",
          "options": [
            "nosuid",
            "noexec",
            "nodev"
          ]
        },
        {
          "destination": "/dev",
          "type": "tmpfs",
          "source": "tmpfs",
          "options": [
            "nosuid",
            "strictatime",
            "mode=755",
            "size=65536k"
          ]
        },
        {
          "destination": "/dev/pts",
          "type": "devpts",
          "source": "devpts",
          "options": [
            "nosuid",
            "noexec",
            "newinstance",
            "ptmxmode=0666",
            "mode=0620",
            "gid=5"
          ]
        },
        {
          "destination": "/dev/mqueue",
          "type": "mqueue",
          "source": "mqueue",
          "options": [
            "nosuid",
            "noexec",
            "nodev"
          ]
        },
        {
          "destination": "/sys",
          "type": "sysfs",
          "source": "sysfs",
          "options": [
            "nosuid",
            "noexec",
            "nodev",
            "ro"
          ]
        },
        {
          "destination": "/sys/fs/cgroup",
          "type": "cgroup",
          "source": "cgroup",
          "options": [
            "nosuid",
            "noexec",
            "nodev",
            "relatime",
            "ro"
          ]
        },
        {
          "destination": "/etc/coredns",
          "type": "bind",
          "source": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~configmap/config-volume",
          "options": [
            "rbind",
            "rprivate",
            "ro"
          ]
        },
        {
          "destination": "/etc/hosts",
          "type": "bind",
          "source": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/etc-hosts",
          "options": [
            "rbind",
            "rprivate",
            "rw"
          ]
        },
        {
          "destination": "/dev/termination-log",
          "type": "bind",
          "source": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/containers/coredns/0db6db2a",
          "options": [
            "rbind",
            "rprivate",
            "rw"
          ]
        },
        {
          "destination": "/etc/hostname",
          "type": "bind",
          "source": "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535/hostname",
          "options": [
            "rbind",
            "rprivate",
            "ro"
          ]
        },
        {
          "destination": "/etc/resolv.conf",
          "type": "bind",
          "source": "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535/resolv.conf",
          "options": [
            "rbind",
            "rprivate",
            "ro"
          ]
        },
        {
          "destination": "/dev/shm",
          "type": "bind",
          "source": "/run/containerd/io.containerd.grpc.v1.cri/sandboxes/3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535/shm",
          "options": [
            "rbind",
            "rprivate",
            "rw"
          ]
        },
        {
          "destination": "/var/run/secrets/kubernetes.io/serviceaccount",
          "type": "bind",
          "source": "/var/lib/kubelet/pods/44e4b37f-504f-4f1c-80f3-4137bfbec5e4/volumes/kubernetes.io~projected/kube-api-access-clt7l",
          "options": [
            "rbind",
            "rprivate",
            "ro"
          ]
        }
      ],
      "hooks": {
        "createContainer": [
          {
            "path": "/kind/bin/mount-product-files.sh"
          }
        ]
      },
      "annotations": {
        "io.kubernetes.cri.container-name": "coredns",
        "io.kubernetes.cri.container-type": "container",
        "io.kubernetes.cri.image-name": "registry.k8s.io/coredns/coredns:v1.10.1",
        "io.kubernetes.cri.sandbox-id": "3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535",
        "io.kubernetes.cri.sandbox-name": "coredns-5d78c9869d-79bfv",
        "io.kubernetes.cri.sandbox-namespace": "kube-system",
        "io.kubernetes.cri.sandbox-uid": "44e4b37f-504f-4f1c-80f3-4137bfbec5e4"
      },
      "linux": {
        "resources": {
          "devices": [
            {
              "allow": false,
              "access": "rwm"
            }
          ],
          "memory": {
            "limit": 178257920,
            "swap": 178257920
          },
          "cpu": {
            "shares": 102,
            "period": 100000
          }
        },
        "cgroupsPath": "kubelet-kubepods-burstable-pod44e4b37f_504f_4f1c_80f3_4137bfbec5e4.slice:cri-containerd:f3a240d625b1e97b859e29f81360fd79538021c46eaafb325dc0fa195c8e4b6a",
        "namespaces": [
          {
            "type": "pid"
          },
          {
            "type": "ipc",
            "path": "/proc/1322/ns/ipc"
          },
          {
            "type": "uts",
            "path": "/proc/1322/ns/uts"
          },
          {
            "type": "mount"
          },
          {
            "type": "network",
            "path": "/proc/1322/ns/net"
          }
        ],
        "maskedPaths": [
          "/proc/asound",
          "/proc/acpi",
          "/proc/kcore",
          "/proc/keys",
          "/proc/latency_stats",
          "/proc/timer_list",
          "/proc/timer_stats",
          "/proc/sched_debug",
          "/proc/scsi",
          "/sys/firmware"
        ],
        "readonlyPaths": [
          "/proc/bus",
          "/proc/fs",
          "/proc/irq",
          "/proc/sys",
          "/proc/sysrq-trigger"
        ]
      }
    }
  }
}
'''

CRICTL_INSPECTI_OUTCOME = r'''
{
  "status": {
    "id": "sha256:b0b1fa0f58c6e932b7f20bf208b2841317a1e8c88cc51b18358310bbd8ec95da",
    "repoTags": [
      "docker.io/kindest/kindnetd:v20230511-dc714da8"
    ],
    "repoDigests": [],
    "size": "27731571",
    "uid": null,
    "username": "",
    "spec": null,
    "pinned": false
  },
  "info": {
    "chainID": "sha256:4a4eab022ba7c07fc20d2ad7b444eccc187c5c0e62ed8492780a1f5ca5f127e7",
    "imageSpec": {
      "created": "2023-05-11T06:58:42.270181933Z",
      "architecture": "amd64",
      "os": "linux",
      "config": {
        "Env": [
          "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        ],
        "Cmd": [
          "/bin/kindnetd"
        ],
        "WorkingDir": "/",
        "ArgsEscaped": true
      },
      "rootfs": {
        "type": "layers",
        "diff_ids": [
          "sha256:e4a5933ff9603ec98b5df28cf7c07c7be52fc15146020cabafb94e0dbb844e19",
          "sha256:4c3e06c130921d5ff04e0f88dd32560b9ce9e887df9db7aa95d0307089d4dcc0",
          "sha256:b60ad5ffce8604e5a03e640797f039b8969a85de95747ef869483655ee21986c",
          "sha256:f671cc97fbe53416a0eb4531a7cc13c93ddc78a4f7635a2342432754bf22810f"
        ]
      },
      "history": [
        {
          "created": "2023-04-06T00:03:08.397579916Z",
          "created_by": "COPY / / # buildkit",
          "comment": "buildkit.dockerfile.v0"
        },
        {
          "created": "2023-05-11T06:58:40.615213899Z",
          "created_by": "COPY ./go/src/kindnetd /bin/kindnetd # buildkit",
          "comment": "buildkit.dockerfile.v0"
        },
        {
          "created": "2023-05-11T06:58:42.236387786Z",
          "created_by": "COPY /_LICENSES/* /LICENSES/ # buildkit",
          "comment": "buildkit.dockerfile.v0"
        },
        {
          "created": "2023-05-11T06:58:42.270181933Z",
          "created_by": "COPY files/LICENSES/* /LICENSES/* # buildkit",
          "comment": "buildkit.dockerfile.v0"
        },
        {
          "created": "2023-05-11T06:58:42.270181933Z",
          "created_by": "CMD [\"/bin/kindnetd\"]",
          "comment": "buildkit.dockerfile.v0",
          "empty_layer": true
        }
      ]
    }
  }
}
'''


def test_pod_sandbox():
    with patch('subprocess.run') as run:
        run.return_value = Mock(stdout=CRICTL_PODS_OUTCOME)
        result = CRIPodSandbox.list()
        assert len(result) == 2
        assert result[0].id == '3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535'
        run.return_value = Mock(stdout=CRICTL_INSPECTP_OUTCOME)
        assert result[0].info['runtimeSpec']['linux']['cgroupsPath'] == 'kubelet-kubepods-burstable-pod44e4b37f_504f_4f1c_80f3_4137bfbec5e4.slice:cri-containerd:3eff2d8d3a4f3d3c0d1c4d4737c21b397bf7cbf77fd8a0fe370f6a59129cd535'


def test_ps():
    with patch('subprocess.run') as run:
        run.return_value = Mock(stdout=CRICTL_PS_OUTCOME)
        result = CRIContainer.list()
        assert len(result) == 2
        assert result[0].id == 'f3a240d625b1e97b859e29f81360fd79538021c46eaafb325dc0fa195c8e4b6a'
        run.return_value = Mock(stdout=CRICTL_INSPECT_OUTCOME)
        assert result[0].info['pid'] == 1478


def test_images():
    with patch('subprocess.run') as run:
        run.return_value = Mock(stdout=CRICTL_IMG_OUTCOME)
        result = CRIImage.list()
        assert len(result) == 2
        result[0].id == 'b0b1fa0f58c6e932b7f20bf208b2841317a1e8c88cc51b18358310bbd8ec95da'
        run.return_value = Mock(stdout=CRICTL_INSPECTI_OUTCOME)
        assert result[0].info['imageSpec']['config']['Cmd'] == ['/bin/kindnetd']
