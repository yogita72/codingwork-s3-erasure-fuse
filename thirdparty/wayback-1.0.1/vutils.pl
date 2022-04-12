#!/usr/bin/perl

use Getopt::Long;
use Pod::Usage;
use Time::Local;
use File::Copy;
use Fcntl;
use strict;

Getopt::Long::Configure ("bundling");

my $number;
my $sincedate;
my $count;
my $list;
my $force;
my $recursive;

if ($0 =~ /vstat$/) {
	$number = 0;
	$sincedate = "";
	$count = 0;
	$list = 0;
	my $help = 0;
	my $result = GetOptions ("number|n=i" => \$number,
				 "date|d=s" => \$sincedate,
				 "count|c=i" => \$count,
				 "list|l" => \$list,
				 "help|h|?" => \$help) or pod2usage(2);
	pod2usage(1) if $help;
	if ($#ARGV < 0) {
		pod2usage(2);
	}
} elsif ($0 =~ /vrevert$/) {
	$number = 0;
	$sincedate = "";
	my $help = 0;
	my $result = GetOptions ("number|n=i" => \$number,
				 "date|d=s" => \$sincedate,
				 "help|h|?" => \$help) or pod2usage(2);
	pod2usage(1) if $help;
	if ($#ARGV < 0) {
	    pod2usage(2);
	}
} elsif ($0 =~ /vextract$/) {
	$number = 0;
	$sincedate = "";
	$force = 0;
	my $help = 0;
	my $result = GetOptions ("number|n=i" => \$number,
				 "date|d=s" => \$sincedate,
				 "force|f" => \$force,
				 "help|h|?" => \$help) or pod2usage(2);
	pod2usage(1) if $help;
	if ($#ARGV != 1) {
	    pod2usage(2);
	}
} elsif ($0 =~ /vrm$/) {
	$force = 0;
	$recursive = 0;
	my $help = 0;
	my $result = GetOptions ("recursive|r" => \$recursive,
				 "force|f" => \$force,
				 "help|h|?" => \$help) or pod2usage(2);
	pod2usage(1) if $help;
	if ($#ARGV < 0) {
	    pod2usage(2);
	}
} else {
	print "$0: Operation unknown, please use a link with one of the below names\n";
	pod2usage(2);
}

vstat() if $0 =~ /vstat$/;
vrevert("", $sincedate, @ARGV) if $0 =~ /vrevert$/;
vrevert(pop @ARGV, $sincedate, @ARGV) if $0 =~ /vextract$/;
vrm(@ARGV) if $0 =~ /vrm$/;

exit(0);

sub vstat {
    foreach my $filename (@ARGV) {
	    $filename =~ s|([^/])$|$1/| if -d $filename;
	    if (-e $filename . ".   versionfs! version") {
		    if (-d $filename) {
			    my %dirinfo = readdirectory($filename . ".   versionfs! version");
			    print "\nDirectory catalog for $filename\:\n---\n" if $#ARGV > 0;
			    $number = scalar @{$dirinfo{time}} if $number == 0;
			    if ($sincedate) {
				my $lookupdate = getdate($sincedate);
				$number = 0;
				for (my $i = 0; $i < scalar @{$dirinfo{time}}; $i++) {
				    if ($dirinfo{time}[$i] >= $lookupdate) {
					$number = (scalar @{$dirinfo{time}}) - $i;
					last;
				    }
				}
			    }
			    $count = scalar @{$dirinfo{time}} if $count == 0;
			    for (my $i = (scalar @{$dirinfo{time}}) - $number; $i < scalar @{$dirinfo{time}} && $count > 0; $i++) {
				next if $i < 0;
				$count--;
				print "#" . ((scalar @{$dirinfo{time}}) - $i) . " at " . scalar localtime($dirinfo{time}[$i]);
				if ($list) {
				    print "\n";
				    next;
				}
				print ":\n\t";
				displaydirentry(\%dirinfo, $i);
			    }
		    } else {
			    my %fileinfo = readfile($filename . ".   versionfs! version");
			    print "\nVersion file for $filename\:\n---\n" if $#ARGV > 0;
			    $number = scalar @{$fileinfo{time}} if $number == 0;
			    if ($sincedate) {
				my $lookupdate = getdate($sincedate);
				$number = 0;
				for (my $i = 0; $i < scalar @{$fileinfo{time}}; $i++) {
				    if ($fileinfo{time}[$i] >= $lookupdate) {
					$number = (scalar @{$fileinfo{time}}) - $i;
					last;
				    }
				}
			    }
			    $count = scalar @{$fileinfo{time}} if $count == 0;
			    for (my $i = (scalar @{$fileinfo{time}}) - $number; $i < scalar @{$fileinfo{time}} && $count > 0; $i++) {
				next if $i < 0;
				$count--;
				print "#" . ((scalar @{$fileinfo{time}}) - $i) . " at " . scalar localtime($fileinfo{time}[$i]);
				if ($list) {
				    print "\n";
				    next;
				}
				print ":\n\t";
				displayfileentry(\%fileinfo, $i);
			    }
		    }
	    } else {
		    print "Version file for $filename does not exist\n";
	    }
    }
}

sub vrevert {
    my $dest = shift;
    my $sincedate = shift;
    foreach my $filename (@_) {
	my $destfile;
	if ($dest) {
	    $destfile = $dest;
	} else {
	    $destfile = $filename;
	}
	$filename =~ s|([^/])$|$1/| if -d $filename;
	    if (-e $filename . ".   versionfs! version") {
		if (-d $filename) {
		    if ($dest) {
			print "You cannot extract a version from a directory!\n";
			last;
		    }
		    my %dirinfo = readdirectory($filename . ".   versionfs! version");
		    $number = scalar @{$dirinfo{time}} if $number == 0;
		    if ($sincedate) {
			my $lookupdate = getdate($sincedate);
			$number = 0;
			for (my $i = 0; $i < scalar @{$dirinfo{time}}; $i++) {
			    if ($dirinfo{time}[$i] >= $lookupdate) {
				$number = (scalar @{$dirinfo{time}}) - $i;
				last;
			    }
			}
		    } else{
			$sincedate = dateformat($dirinfo{'time'}[max(scalar @{$dirinfo{time}} - $number, 0)]);
		    }
		    for (my $i = (scalar @{$dirinfo{time}}) - 1; $i >= scalar @{$dirinfo{time}} - $number; $i--) {
			last if $i < 0;
			revertdirentry(\%dirinfo, $i, $filename);
		    }
		    vrevert("", $sincedate, getdircontents($filename));
	        } else {
	    	    my %fileinfo = readfile($filename . ".   versionfs! version");
		    $number = scalar @{$fileinfo{time}} if $number == 0;
		    if ($sincedate) {
			my $lookupdate = getdate($sincedate);
			$number = 0;
			for (my $i = 0; $i < scalar @{$fileinfo{time}}; $i++) {
			    if ($fileinfo{time}[$i] >= $lookupdate) {
				$number = (scalar @{$fileinfo{time}}) - $i;
				last;
			    }
			}
		    }
		    if ($dest) {
			next if not copyfile($filename, $destfile);
		    }
		    for (my $i = (scalar @{$fileinfo{time}}) - 1; $i >= scalar @{$fileinfo{time}} - $number; $i--) {
			last if $i < 0;
			revertfileentry(\%fileinfo, $i, $destfile);
		    }
	        }
	    } else {
		    print "Version file for $filename does not exist\n" if (("," . join("/,", @ARGV) . "/,") =~ /,$filename\/?,/);
	    }
    }
}

sub vrm {
    foreach my $filename (@_) {
        $filename =~ s|([^/])$|$1/| if -d $filename;
	if (-d $filename and $recursive) {
	    vrm(getdircontents($filename));
	}
        if (-e $filename . ".   versionfs! version") {
	    if ($force || askyn("Delete version info for $filename?")) {
	        unlink $filename . ".   versionfs! version";
	    }
	} else {
	    print "Version file for $filename does not exist\n" if (("," . join("/,", @ARGV) . "/,") =~ /,$filename\/?,/);
        }
    }
}

sub dateformat {
    my @sincedate = localtime($_[0]);
    return ($sincedate[5] + 1900) . ":" . ($sincedate[4] + 1) . ":$sincedate[3]:$sincedate[2]:$sincedate[1]:$sincedate[0]";
}

sub max {
    return ($_[0] > $_[1]) ? $_[0] : $_[1];
}

sub copyfile {
    my $srcfile = shift;
    my $dstfile = shift;
    if (-e $dstfile) {
	return 0 if askyn("Overwrite existing $dstfile?");
    }
    if (not -e $srcfile) {
	open(TMP, ">$dstfile") or (print "Error creating file $dstfile ($!)\n" and return 0);
	close TMP;
	return 1;
    }
    copy($srcfile, $dstfile) or (print "Error copying file $srcfile to $dstfile ($!)\n" and return 0);
    return 1;
}

sub askyn {
    my $question = shift;
    my $answer = "";
    print "$question (y/n): ";
    while ($answer !~ /^(y(es)?|no?)$/i) {
	$answer = <STDIN>;
	print "Please answer yes or no: " if ($answer !~ /^(y(es)?|no?)$/i);
    }
    return 1 if $answer =~ /^y(es)?$/i;
    return 0;
}

sub getdircontents {
    my $dirname = shift;
    opendir(DIR, $dirname) or return ();
    my @allfiles = readdir DIR;
    closedir DIR;
    return @allfiles;
}

sub revertdirentry {
    my $dirinfo = shift;	#Hash reference
    my $index = shift;
    my $dirname = shift;
    my $action = ${$dirinfo}{'action'}[$index];
    if ($action == 1) {				#Create
	unlink $dirname . ${$dirinfo}{'file1'}[$index];
    } elsif ($action == 4) {			#Mkdir
	vrevert("", dateformat(${$dirinfo}{'time'}[$index]), $dirname . ${$dirinfo}{'file1'}[$index]);
	rmdir $dirname . ${$dirinfo}{'file1'}[$index];
    } elsif ($action == 2) {			#Rm
	if (${$dirinfo}{'stats'}[$index]{'mode'} & 0120000 == 0120000) {
	    symlink $dirname . ${$dirinfo}{'file2'}[$index], ${$dirinfo}{'file1'}[$index];
	} else {
	    open(TMP, ">$dirname${$dirinfo}{'file1'}[$index]") or (print "Could not create $dirname${$dirinfo}{'file1'}[$index] ($!)\n" and return);
	    close(TMP);
	}
	chmod 07777 & ${$dirinfo}{'stats'}[$index]{'mode'}, $dirname . ${$dirinfo}{'file1'}[$index];
	chown ${$dirinfo}{'stats'}[$index]{'uid'}, ${$dirinfo}{'stats'}[$index]{'gid'}, $dirname . ${$dirinfo}{'file1'}[$index];
	utime ${$dirinfo}{'stats'}[$index]{'atime'}, ${$dirinfo}{'stats'}[$index]{'mtime'}, $dirname . ${$dirinfo}{'file1'}[$index];
    } elsif ($action == 5 or $action == 8) {	#Rename/Rmdir
	vrevert("", dateformat(${$dirinfo}{'time'}[$index]), $dirname . ${$dirinfo}{'file2'}[$index]);
	rename $dirname . ${$dirinfo}{'file2'}[$index], $dirname . ${$dirinfo}{'file1'}[$index];
    } elsif ($action == 12) {			#Chmod
	chmod 07777 & ${$dirinfo}{'stats'}[$index]{'mode'}, $dirname . ${$dirinfo}{'file1'}[$index];	
    } elsif ($action == 13) {			#Chown
	chown ${$dirinfo}{'stats'}[$index]{'uid'}, ${$dirinfo}{'stats'}[$index]{'gid'}, $dirname . ${$dirinfo}{'file1'}[$index];	
    } elsif ($action == 14) {			#Utime
	utime ${$dirinfo}{'stats'}[$index]{'atime'}, ${$dirinfo}{'stats'}[$index]{'mtime'}, $dirname . ${$dirinfo}{'file1'}[$index];
    } else {
	print "Action $action unknown!!!\n";
    }
}

sub revertfileentry {
    my $fileinfo = shift;
    my $index = shift;
    my $dstfile = shift;
    sysopen(DST, $dstfile, O_WRONLY | O_CREAT);
    sysseek DST, ${$fileinfo}{'offset'}[$index], 0;
		if (${$fileinfo}{'moreoffset'}[$index] > 0) {
			my $mo = ${$fileinfo}{'moreoffset'}[$index];
			while ($mo > 0) {
				sysseek DST, 2 ** 32, 1;
				$mo--;
			}
		}
    my $len = ${$fileinfo}{'size'}[$index];
    my $offset = 0;
    while ($len) {
			my $written = syswrite DST, ${$fileinfo}{'data'}[$index], $len, $offset;
			return unless defined $written;
			$offset += $written;
			$len -= $written;
    }
    if (${$fileinfo}{'truncate'}[$index]) {
			truncate DST, ${$fileinfo}{'offset'}[$index] + ${$fileinfo}{'size'}[$index];
    }
}

sub readdirectory {
    my $filename = $_[0];
    my %dirinfo = ('time' => [],
		   'action' => [],
		   'stats' => [],
		   'file1' => [],
		   'file2' => []);
    open (VERFILE, "<$filename") || (print "Could not open directory catalog $filename ($!)\n" && return ());
    binmode VERFILE;
    while (!eof VERFILE) {
	last if not read(VERFILE, my $numericdata, 12);
	(my $time, my $action, my $datasize) = unpack("III", $numericdata);
	my $file1 = ""; my $file2 = ""; my %stats = ();
	last if not read(VERFILE, my $data, $datasize);
	if ($action == 1 or $action == 4) { #CREATE/MKDIR
	    ($file1) = unpack("Z*", $data);
	} elsif ($action == 2) { #RM
	    my @stats = unpack("IIIIII", $data);
	    if ($stats[0] & 0120000 == 0120000) {
		my @data = unpack("IIIIIIZ*Z*", $data);
		$file2 = pop @data;
		$file1 = pop @data;
	    } else {
		my @data = unpack("IIIIIIZ*", $data);
		$file1 = pop @data;
	    }
	    %stats = ('mode'	 =>  $stats[0],
		      'uid'	 =>  $stats[1],
		      'gid'	 =>  $stats[2],
		      'atime'    =>  $stats[3],
		      'mtime'    =>  $stats[4],
		      'ctime'    =>  $stats[5]);
	} elsif ($action == 5 or $action == 8) { #RENAME/RMDIR
	    ($file1, $file2) = unpack("Z*Z*", $data);
	} elsif ($action >= 12 and $action <= 14) { #CHMOD/CHOWN/UTIME
	    my @stats = unpack("IIIIIIZ*", $data);
	    %stats = ('mode'	 =>  $stats[0],
		      'uid'	 =>  $stats[1],
		      'gid'	 =>  $stats[2],
		      'atime'    =>  $stats[3],
		      'mtime'    =>  $stats[4],
		      'ctime'    =>  $stats[5]);
	    $file1 = $stats[6];
	} else {
	    print "Action unknown: $action!!\n";
	}
	push @{$dirinfo{'time'}}, $time;
	push @{$dirinfo{'action'}}, $action;
	push @{$dirinfo{'stats'}}, {};
	%{$dirinfo{'stats'}[scalar @{$dirinfo{'stats'}} - 1]} = %stats;
	$file1 =~ s/\0//g;
	push @{$dirinfo{'file1'}}, $file1;
	$file2 =~ s/\0//g;
	push @{$dirinfo{'file2'}}, $file2;
    }
    close VERFILE;
    return %dirinfo;
}

sub readfile {
    my $filename = $_[0];
    my %fileinfo = ('time' => [],
		    'offset' => [],
				'moreoffset' => [],
		    'size' => [],
		    'truncate' => [],
		    'data' => []);
    open (VERFILE, "<$filename") || (print "Could not open version file $filename ($!)\n" && return ());
    binmode VERFILE;
    while (!eof VERFILE) {
			last if not read(VERFILE, my $numericdata, 17);
			(my $time, my $offset, my $moreoffset, my $size, my $trunc) = unpack("IIIIC", $numericdata);
			my $data = "";
			if ($size > 0) {
	  	  last if !read(VERFILE, $data, $size);
			}
			push @{$fileinfo{'time'}}, $time;
			push @{$fileinfo{'offset'}}, $offset;
			push @{$fileinfo{'moreoffset'}}, $moreoffset;
			push @{$fileinfo{'size'}}, $size;
			push @{$fileinfo{'truncate'}}, $trunc;
			push @{$fileinfo{'data'}}, $data;
    }
    close VERFILE;
    return %fileinfo;
}

sub displaydirentry {
    my %dirinfo = %{$_[0]};
    my $index = $_[1];
    my %actions = ( 1	=>  "Created {file1}\n",
		    2	=>  "Deleted {file1}\n",
		    'symlink' => "Removed link {file1} which pointed to {file2}\n",
		    4	=>  "Made direcotry {file1}\n",
		    5	=>  "Removed directory {file1} (and renamed to {file2})\n",
		    8	=>  "Renamed {file1} to {file2}\n",
		    12	=>  "Changed mode of {file1}\n",
		    13	=>  "Changed ownership of {file1}\n",
		    14	=>  "Changed times of {file1}\n");
    my $actionstring = $actions{$dirinfo{'action'}[$index]};
    $actionstring = $actions{'symlink' } if $dirinfo{'action'}[$index] == 2 and ($dirinfo{'stats'}[$index]{'mode'} & 0120000) == 0120000;
    $actionstring =~ s/\{(.*?)\}/$dirinfo{$1}[$index]/eg;
    print $actionstring;
}

sub displayfileentry {
    my %fileinfo = %{$_[0]};
    my $index = $_[1];
    print "$fileinfo{size}[$index] bytes overwritten/truncated at offset " . (($fileinfo{moreoffset}[$index] == 0) ? $fileinfo{offset}[$index] : "$fileinfo{moreoffset}[$index]*2^32 + $fileinfo{offset}[$index]");
    print " and file extended" if $fileinfo{'truncate'}[$index];
    print "\n";
}

sub getdate {
    my $sincedate = $_[0];
    if ($sincedate =~ /^(\d{1,4}\:){0,5}\d{1,2}$/) {
	my @sincedate = split(/\:/, $sincedate);
	my $fieldsgiven = $#sincedate;
        my @nowdate = localtime;
        my @lookupdate = (@nowdate[0 .. 5]);
        @sincedate = reverse @sincedate;
	$sincedate[4]-- if $sincedate[4];
	$sincedate[5] = ($sincedate[5] >= 100) ? $sincedate[5] - 1900 : ($sincedate[5] > 70) ? $sincedate[5] : $sincedate[5] + 100 if $sincedate[5];
	for (my $i = 0; $i <= $fieldsgiven; $i++) {
	    $lookupdate[$i] = $sincedate[$i];
	}
	return timelocal(@lookupdate);
    } else {
	print "Invalid date format $sincedate!\n";
	return time;
    }
}

__END__

=head1 NAME

vstat - list version information for a file or files
vrevert - revert a file or files to a previous state
vextract - extract a previous state from a file to a new file
vrm - remove versioning information for a file or files

=head1 SYNOPSIS

vstat [I<options>] I<file> [I<file2 ...>]

 Options:
   -n,--number	n	Number of log entries to look back
   -d,--date	d	Date to look back to
   -c,--count	c	Number of entries to display
   -l,--list		List entry times only
   -h,-?,--help		Brief help message

vrevert [I<options>] I<file> [I<file2 ...>]

 Options:
   -n,--number	n	Number of the log entry to revert to
   -d,--date	d	Date to revert back to
   -h,-?,--help		Brief help message

vextract [I<options>] I<source> I<destination>

 Options:
   -n,--number	n	Number of the log entry to extract
   -d,--date	d	Date prior to extract state for
   -f,--force		Overwrite destination without asking
   -h,-?,--help		Brief help message

vrm [I<options>] I<file> [I<file2 ...>]

 Options:
   -r,--recursive	If I<file> is a directory, delete all version within it
   -f,--force		Do not ask before deleting
   -h,-?,--help		Brief help message
 
=head1 OPTIONS

=head2 vstat

=over 4

=item B<-n>,B<--number> I<n>

Start printing entries with the I<n>th most recent entry in the version log.

=item B<-d>,B<--date> [[[[[I<yyyy>:]I<mm>:]I<dd>:]I<hh>:]I<mm>:]I<ss>

Start printing entries that happen after the time given in the above format. Overrides
the B<-n> option.

=item B<-c>,B<--count> I<n>

Print only I<n> entries beginning either at the beginning of the file (oldest), or
where specified by the B<-n> or B<-d> options.

=item B<-l>,B<--list>

Do not print detailed information about each entry, only print the time at which
entries were recorded.

=item B<-h>,B<-?>,B<--help>

Print information about how to use the vstat, vrevert, vextract, and vrm utilities.

=item I<file>, I<file2>, I<...>

File or directory to print version information about

=back

=head2 vrevert

=over 4

=item B<-n>,B<--number> I<n>

Revert all operations after and including the I<n>th most recent operation (as listed
by vstat).

=item B<-d>,B<--date> [[[[[I<yyyy>:]I<mm>:]I<dd>:]I<hh>:]I<mm>:]I<ss>

Revert the file or directory back to the state at which it should have been at the date
and time specified in the above format. Overrides the B<-n> option.

=item B<-h>,B<-?>,B<--help>

Print information about how to use the vstat, vrevert, vextract, and vrm utilities.

=item I<file>, I<file2>, I<...>

File or directory to revert back to an earlier state. Note, if a directory is reverted,
all files and directories contained within it will also be reverted.

=back

=head2 vextract

=over 4

=item B<-n>,B<--number> I<n>

Extract the state of the source file before the I<n>th most recent operation (as listed
by vstat).

=item B<-d>,B<--date> [[[[[I<yyyy>:]I<mm>:]I<dd>:]I<hh>:]I<mm>:]I<ss>

Extract the stat of the source file as it should have been at the date and time
specified in the above format. Overrrides the B<-n> option.

=item B<-f>,B<--force>

Do not ask before overwriting a file with the same name as the destination.

=item B<-h>,B<-?>,B<--help>

Print information about how to use the vstat, vrevert, vextract, and vrm utilities.

=item I<source>

File to extract an earlier version from. Directories may not be specified.

=item I<destination>

Destination file name to save the extracted version as.

=back

=head2 vrm

=over 4

=item B<-r>,B<--recursive>

Delete all versioning information contained within a directory and all subdirectories
of that directory. Note, this will only delete versioning information for files that
currently exist; versioning information for files that have since been deleted will
remain.

=item B<-f>,B<--force>

Do not ask for confirmation before deleting versioning information.

=item B<-h>,B<-?>,B<--help>

Print information about how to use the vstat, vrevert, vextract, and vrm utilities.

=item I<file>,I<file2>,I<...>

Files to remove versioning information for.

=cut
